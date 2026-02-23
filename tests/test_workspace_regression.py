import importlib.util
import json
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

    def test_load_config_with_managed_target_overrides(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        managed_path = config_dir / "source_targets.json"
        managed_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "targets": [
                        {
                            "id": "storiesofcz",
                            "watch_kind": "posts",
                            "target_handle": "storiesofcz",
                            "enabled": False,
                            "url": "https://www.tiktok.com/@storiesofcz",
                        },
                        {
                            "id": "storiesofcz_likes",
                            "watch_kind": "likes",
                            "target_handle": "storiesofcz",
                            "enabled": True,
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )

        _, sources = self.mod.load_config(config_path)
        by_id = {source.id: source for source in sources}
        self.assertIn("storiesofcz", by_id)
        self.assertIn("storiesofcz_likes", by_id)

        override_source = by_id["storiesofcz"]
        self.assertFalse(override_source.enabled)
        self.assertEqual(override_source.origin, "managed_override")

        likes_source = by_id["storiesofcz_likes"]
        self.assertTrue(likes_source.enabled)
        self.assertEqual(likes_source.watch_kind, "likes")
        self.assertEqual(likes_source.target_handle, "storiesofcz")
        self.assertEqual(likes_source.url, "https://www.tiktok.com/@storiesofcz/liked")
        self.assertEqual(likes_source.origin, "managed")

    def test_source_target_api_upsert_and_remove(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
            """.strip()
            + "\n",
            encoding="utf-8",
        )

        handler_class = self.mod.build_web_handler(
            db_path=self.db_path,
            static_dir=self.mod.WEB_STATIC_DIR,
            allowed_source_ids=set(),
            config_path=config_path,
            restrict_to_source_ids=False,
        )
        server = self.mod.ThreadingHTTPServer(("127.0.0.1", 0), handler_class)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(lambda: thread.join(timeout=3))

        host, port = server.server_address
        upsert_url = f"http://{host}:{port}/api/source-targets/upsert"
        list_url = f"http://{host}:{port}/api/source-targets"
        remove_url = f"http://{host}:{port}/api/source-targets/remove"

        upsert_payload = {
            "id": "storiesofcz_likes",
            "watch_kind": "likes",
            "target_handle": "storiesofcz",
            "enabled": True,
        }
        request = urllib.request.Request(
            upsert_url,
            data=json.dumps(upsert_payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=5) as response:
            self.assertEqual(response.status, 200)
            payload = json.loads(response.read().decode("utf-8"))
        self.assertIn(payload.get("status"), {"created", "updated"})

        with urllib.request.urlopen(list_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            list_payload = json.loads(response.read().decode("utf-8"))
        by_id = {row["id"]: row for row in list_payload.get("targets", [])}
        self.assertIn("storiesofcz_likes", by_id)
        self.assertEqual(by_id["storiesofcz_likes"]["watch_kind"], "likes")
        self.assertEqual(by_id["storiesofcz_likes"]["url"], "https://www.tiktok.com/@storiesofcz/liked")

        remove_request = urllib.request.Request(
            remove_url,
            data=json.dumps({"id": "storiesofcz_likes"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(remove_request, timeout=5) as response:
            self.assertEqual(response.status, 200)
            remove_payload = json.loads(response.read().decode("utf-8"))
        self.assertEqual(remove_payload.get("status"), "removed")

        with urllib.request.urlopen(list_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            list_payload_after = json.loads(response.read().decode("utf-8"))
        by_id_after = {row["id"]: row for row in list_payload_after.get("targets", [])}
        self.assertNotIn("storiesofcz_likes", by_id_after)

    def test_build_media_audio_fallback_format_selector_prefers_download_then_audio(self):
        selector = self.mod.build_media_audio_fallback_format_selector("bv*+ba/best")
        self.assertEqual(selector, "download/best*[acodec!=none]/bv*+ba/best")

        blank_selector = self.mod.build_media_audio_fallback_format_selector("  ")
        self.assertEqual(blank_selector, "download/best*[acodec!=none]/best")

    def test_build_media_audio_fallback_format_candidates_include_audio_safe_fallbacks(self):
        candidates = self.mod.build_media_audio_fallback_format_candidates("bv*+ba/best")
        self.assertEqual(candidates[0], "download/best*[acodec!=none]/bv*+ba/best")
        self.assertIn("best*[acodec!=none][format_id*=h264]/best*[acodec!=none]", candidates)
        self.assertIn("best*[acodec!=none]/best", candidates)
        self.assertEqual(len(candidates), len(set(candidates)))

        preferred = "best*[acodec!=none][format_id*=h264]/best*[acodec!=none]"
        preferred_candidates = self.mod.build_media_audio_fallback_format_candidates(
            "bv*+ba/best",
            preferred_format=preferred,
        )
        self.assertEqual(preferred_candidates[0], preferred)
        self.assertEqual(len(preferred_candidates), len(set(preferred_candidates)))

    def test_media_fallback_preferred_format_state_roundtrip(self):
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)

            self.assertIsNone(
                self.mod.get_media_fallback_preferred_format(connection, "storiesofcz")
            )

            self.mod.record_media_fallback_preferred_format(
                connection=connection,
                source_id="storiesofcz",
                preferred_format="fmt_a",
                updated_at="2026-02-22T12:00:00+00:00",
            )
            self.assertEqual(
                self.mod.get_media_fallback_preferred_format(connection, "storiesofcz"),
                "fmt_a",
            )

            self.mod.record_media_fallback_preferred_format(
                connection=connection,
                source_id="storiesofcz",
                preferred_format="fmt_a",
                updated_at="2026-02-22T12:01:00+00:00",
            )
            row_same = connection.execute(
                "SELECT preferred_format, success_count FROM media_fallback_format_state WHERE source_id = ?",
                ("storiesofcz",),
            ).fetchone()
            self.assertEqual(row_same[0], "fmt_a")
            self.assertEqual(int(row_same[1]), 2)

            self.mod.record_media_fallback_preferred_format(
                connection=connection,
                source_id="storiesofcz",
                preferred_format="fmt_b",
                updated_at="2026-02-22T12:02:00+00:00",
            )
            row_new = connection.execute(
                "SELECT preferred_format, success_count FROM media_fallback_format_state WHERE source_id = ?",
                ("storiesofcz",),
            ).fetchone()
            self.assertEqual(row_new[0], "fmt_b")
            self.assertEqual(int(row_new[1]), 1)
        finally:
            connection.close()

    def test_feed_sources_include_configured_sources_without_media(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true

[[sources]]
id = "ortbake"
platform = "tiktok"
url = "https://www.tiktok.com/@ortbake/liked"
watch_kind = "likes"
target_handle = "ortbake"
enabled = true
            """.strip()
            + "\n",
            encoding="utf-8",
        )

        handler_class = self.mod.build_web_handler(
            db_path=self.db_path,
            static_dir=self.mod.WEB_STATIC_DIR,
            allowed_source_ids=set(),
            config_path=config_path,
            restrict_to_source_ids=False,
        )
        server = self.mod.ThreadingHTTPServer(("127.0.0.1", 0), handler_class)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(lambda: thread.join(timeout=3))

        host, port = server.server_address
        feed_url = f"http://{host}:{port}/api/feed?limit=20&offset=0"
        with urllib.request.urlopen(feed_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            payload = json.loads(response.read().decode("utf-8"))

        self.assertEqual(payload.get("count"), 0)
        self.assertEqual(payload.get("videos"), [])
        source_ids = set(payload.get("sources", []))
        self.assertIn("storiesofcz", source_ids)
        self.assertIn("ortbake", source_ids)


if __name__ == "__main__":
    unittest.main()
