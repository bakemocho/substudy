import importlib.util
import sqlite3
import sys
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path


def load_substudy_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "substudy.py"
    spec = importlib.util.spec_from_file_location("substudy_workspace_test", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load scripts/substudy.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class WorkspaceRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_substudy_module()

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.workspace_root = Path(self.tempdir.name) / "workspace"
        (self.workspace_root / "data").mkdir(parents=True, exist_ok=True)
        (self.workspace_root / "exports" / "llm").mkdir(parents=True, exist_ok=True)
        self.db_path = self.workspace_root / "data" / "master_ledger.sqlite"
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            connection.commit()
        finally:
            connection.close()

    def test_review_hint_and_translation_qa_join(self):
        hints_path = self.workspace_root / "exports" / "llm" / "review_hints.jsonl"
        hints_path.write_text(
            '{"card_id":"dictbm:10","one_line_hint_ja":"ja hint","one_line_hint_en":"en hint"}\n',
            encoding="utf-8",
        )
        qa_path = self.workspace_root / "exports" / "llm" / "translation_qa.jsonl"
        qa_path.write_text(
            '{"card_id":"dictbm:10","qa_result":"check","reason":"too_short_vs_en"}\n'
            '{"card_id":"dictbm:11","qa_result":"ok","reason":"ok"}\n',
            encoding="utf-8",
        )

        review_cards = [{"card_id": "dictbm:10", "definition": "def"}]
        missing_cards = [
            {"card_id": "dictbm:10", "definition": "辞書エントリが見つかりません。"},
            {"card_id": "dictbm:12", "definition": "custom definition"},
        ]

        hints = self.mod.load_workspace_review_hints(self.workspace_root)
        qa_map = self.mod.load_workspace_translation_qa(self.workspace_root)
        review_cards = self.mod.apply_workspace_review_hints(review_cards, hints)
        review_cards = self.mod.apply_workspace_translation_qa(review_cards, qa_map)
        missing_cards = self.mod.apply_workspace_translation_qa(missing_cards, qa_map)
        missing_cards = self.mod.apply_workspace_missing_entry_states(missing_cards)

        self.assertEqual(review_cards[0]["one_line_hint_ja"], "ja hint")
        self.assertEqual(review_cards[0]["one_line_hint_en"], "en hint")
        self.assertEqual(review_cards[0]["qa_result"], "check")
        self.assertEqual(review_cards[0]["qa_reason"], "too_short_vs_en")

        self.assertEqual(missing_cards[0]["missing_status"], "needs_review")
        self.assertEqual(missing_cards[0]["missing_status_label"], "要再確認")
        self.assertEqual(missing_cards[1]["missing_status"], "enriched")
        self.assertEqual(missing_cards[1]["missing_status_label"], "補完済み")

    def test_workspace_artifact_kind_and_urls(self):
        review_hints_path = self.workspace_root / "exports" / "llm" / "review_hints.jsonl"
        translation_qa_path = self.workspace_root / "exports" / "llm" / "translation_qa.jsonl"
        review_hints_path.write_text("{}", encoding="utf-8")
        translation_qa_path.write_text("{}", encoding="utf-8")

        artifacts = self.mod.collect_workspace_artifacts(self.workspace_root, limit=10)
        by_name = {row["name"]: row for row in artifacts}
        self.assertIn("review_hints.jsonl", by_name)
        self.assertIn("translation_qa.jsonl", by_name)
        self.assertEqual(by_name["review_hints.jsonl"]["kind"], "review_hints")
        self.assertEqual(by_name["translation_qa.jsonl"]["kind"], "translation_qa")
        self.assertTrue(str(by_name["review_hints.jsonl"]["open_url"]).startswith("/artifact/"))
        self.assertIn("?download=1", str(by_name["review_hints.jsonl"]["download_url"]))

    def test_import_monitor_records_latest_summary(self):
        input_path = self.workspace_root / "exports" / "llm" / "enriched_missing.jsonl"
        input_path.write_text("", encoding="utf-8")
        self.mod.run_dict_bookmarks_import(
            db_path=self.db_path,
            source_ids=["storiesofcz"],
            input_path=input_path,
            input_format="jsonl",
            on_duplicate="upsert",
            dry_run=True,
        )

        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        try:
            monitor = self.mod.collect_workspace_import_monitor(
                connection=connection,
                source_ids=["storiesofcz"],
                run_limit=6,
            )
        finally:
            connection.close()

        self.assertIsNotNone(monitor["latest"])
        self.assertEqual(monitor["latest"]["status"], "noop")
        self.assertTrue(monitor["latest"]["dry_run"])
        self.assertEqual(monitor["latest"]["row_count"], 0)
        self.assertGreaterEqual(len(monitor["recent_runs"]), 1)

    def test_artifact_open_and_download_headers(self):
        artifact_path = self.workspace_root / "exports" / "llm" / "sample.jsonl"
        artifact_body = '{"hello":"world"}\n'
        artifact_path.write_text(artifact_body, encoding="utf-8")
        token = self.mod.encode_path_token(artifact_path.resolve())

        handler_class = self.mod.build_web_handler(
            db_path=self.db_path,
            static_dir=self.mod.WEB_STATIC_DIR,
            allowed_source_ids=set(),
        )
        server = self.mod.ThreadingHTTPServer(("127.0.0.1", 0), handler_class)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(lambda: thread.join(timeout=3))

        host, port = server.server_address
        open_url = f"http://{host}:{port}/artifact/{token}"
        download_url = f"http://{host}:{port}/artifact/{token}?download=1"

        with urllib.request.urlopen(open_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers.get_content_type(), "text/plain")
            self.assertTrue(
                str(response.headers.get("Content-Disposition", "")).startswith("inline;")
            )
            body = response.read().decode("utf-8")
            self.assertEqual(body, artifact_body)

        with urllib.request.urlopen(download_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            self.assertTrue(
                str(response.headers.get("Content-Disposition", "")).startswith("attachment;")
            )


if __name__ == "__main__":
    unittest.main()
