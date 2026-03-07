import importlib.util
import json
import sqlite3
import sys
import tempfile
import threading
import unittest
import datetime as dt
from unittest import mock
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
        self.mod._YTDLP_IMPERSONATE_TARGETS_CACHE.clear()
        self.mod._YTDLP_IMPERSONATE_WARNED_KEYS.clear()
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

    def test_load_config_resolves_ytdlp_bin_to_absolute_path(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"
ytdlp_bin = "yt-dlp"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
            """.strip()
            + "\n",
            encoding="utf-8",
        )

        def fake_which(name):
            if name == "yt-dlp":
                return "/opt/homebrew/bin/yt-dlp"
            return None

        with mock.patch.object(self.mod.shutil, "which", side_effect=fake_which):
            _, sources = self.mod.load_config(config_path)
        self.assertEqual(sources[0].ytdlp_bin, "/opt/homebrew/bin/yt-dlp")

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

    def test_load_config_parses_sleep_requests_and_media_interval_overrides(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"
source_order = "random"
sleep_requests = 1.5
media_discovery_interval_hours = 24

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true

[[sources]]
id = "storiesofcz_likes"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz/liked"
watch_kind = "likes"
enabled = true
sleep_requests = 0.25
media_discovery_interval_hours = 3
            """.strip()
            + "\n",
            encoding="utf-8",
        )

        global_config, sources = self.mod.load_config(config_path)
        by_id = {source.id: source for source in sources}
        self.assertEqual(global_config.source_order, "random")
        self.assertAlmostEqual(float(by_id["storiesofcz"].sleep_requests), 1.5, places=3)
        self.assertAlmostEqual(float(by_id["storiesofcz_likes"].sleep_requests), 0.25, places=3)
        self.assertAlmostEqual(
            float(by_id["storiesofcz"].media_discovery_interval_hours),
            24.0,
            places=3,
        )
        self.assertAlmostEqual(
            float(by_id["storiesofcz_likes"].media_discovery_interval_hours),
            3.0,
            places=3,
        )

    def test_order_sources_for_run_random_keeps_same_members(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"

[[sources]]
id = "a"
platform = "tiktok"
url = "https://www.tiktok.com/@a"
enabled = true

[[sources]]
id = "b"
platform = "tiktok"
url = "https://www.tiktok.com/@b"
enabled = true

[[sources]]
id = "c"
platform = "tiktok"
url = "https://www.tiktok.com/@c"
enabled = true
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)

        def reverse_in_place(items):
            items[:] = list(reversed(items))

        with mock.patch.object(self.mod.random, "shuffle", side_effect=reverse_in_place):
            ordered = self.mod.order_sources_for_run(
                sources=sources,
                mode="random",
                command_name="sync",
            )

        self.assertEqual([source.id for source in ordered], ["c", "b", "a"])
        self.assertEqual({source.id for source in ordered}, {"a", "b", "c"})

    def test_discover_playlist_window_ids_uses_sleep_requests_flags(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"
sleep_interval = 3
max_sleep_interval = 7
retry_sleep = 9
sleep_requests = 1.25

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        captured: dict[str, list[str]] = {}

        def fake_run(command, check, capture_output, text):
            captured["command"] = list(command)
            return self.mod.subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout="7611111111111111111\n7612222222222222222\n",
                stderr="",
            )

        with mock.patch.object(self.mod.subprocess, "run", side_effect=fake_run):
            ids = self.mod.discover_playlist_window_ids(
                source=source,
                playlist_start=1,
                playlist_end=5,
                dry_run=False,
            )

        self.assertEqual(ids, ["7611111111111111111", "7612222222222222222"])
        command = captured["command"]
        self.assertIn("--sleep-requests", command)
        self.assertIn("1.25", command)
        self.assertIn("--sleep-interval", command)
        self.assertIn("--max-sleep-interval", command)
        self.assertIn("--retry-sleep", command)
        self.assertNotIn("--ignore-errors", command)

    def test_resolve_impersonate_flags_auto_prefers_chrome(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"
ytdlp_impersonate = "auto"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        with mock.patch.object(
            self.mod,
            "list_ytdlp_impersonate_targets",
            return_value=["firefox", "chrome"],
        ):
            flags = self.mod.resolve_impersonate_flags(source)

        self.assertEqual(flags, ["--impersonate", "chrome"])

    def test_discover_playlist_window_ids_includes_impersonate_flag(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"
ytdlp_impersonate = "chrome"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        captured: dict[str, list[str]] = {}

        def fake_run(command, check, capture_output, text):
            captured["command"] = list(command)
            return self.mod.subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout="7611111111111111111\n",
                stderr="",
            )

        with (
            mock.patch.object(
                self.mod,
                "list_ytdlp_impersonate_targets",
                return_value=["chrome"],
            ),
            mock.patch.object(self.mod.subprocess, "run", side_effect=fake_run),
        ):
            ids = self.mod.discover_playlist_window_ids(
                source=source,
                playlist_start=1,
                playlist_end=1,
                dry_run=False,
            )

        self.assertEqual(ids, ["7611111111111111111"])
        command = captured["command"]
        self.assertIn("--impersonate", command)
        self.assertIn("chrome", command)

    def test_schedule_next_retry_iso_uses_longer_backoff_for_blocked_error(self):
        before = dt.datetime.now(dt.timezone.utc)
        blocked_retry_at = dt.datetime.fromisoformat(
            self.mod.schedule_next_retry_iso(
                1,
                error_message="ERROR: [TikTok] Your IP address is blocked from accessing this post",
            )
        )
        after = dt.datetime.now(dt.timezone.utc)
        blocked_delay_min = (blocked_retry_at - before).total_seconds()
        blocked_delay_max = (blocked_retry_at - after).total_seconds()
        self.assertGreaterEqual(blocked_delay_min, (6 * 3600) - 5)
        self.assertLessEqual(blocked_delay_max, (6 * 3600) + 5)

        normal_retry_at = dt.datetime.fromisoformat(
            self.mod.schedule_next_retry_iso(
                1,
                error_message="temporary downloader error",
            )
        )
        normal_delay = (normal_retry_at - after).total_seconds()
        self.assertGreaterEqual(normal_delay, 295)
        self.assertLessEqual(normal_delay, 305)

    def test_create_schema_includes_parallel_queue_tables(self):
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            self.assertIn("work_items", tables)
            self.assertIn("source_poll_state", tables)
            self.assertIn("worker_heartbeats", tables)
        finally:
            connection.close()

    def test_get_queue_producer_lock_path_uses_db_sibling_locks_dir(self):
        db_path = self.workspace_root / "data" / "custom.sqlite"
        lock_path = self.mod.get_queue_producer_lock_path(db_path)
        expected = db_path.resolve().parent / "locks" / self.mod.DEFAULT_PRODUCER_LOCK_FILE_NAME
        self.assertEqual(lock_path, expected)

    def test_queue_producer_lock_writes_holder_and_clears_on_release(self):
        if self.mod.fcntl is None:
            self.skipTest("fcntl unavailable")

        lock_path = self.workspace_root / "data" / "locks" / "producer.lock"
        with self.mod.queue_producer_lock(lock_path, enabled=True):
            holder = lock_path.read_text(encoding="utf-8").strip()
            self.assertIn("pid=", holder)
            self.assertIn("host=", holder)
            self.assertIn("started_at=", holder)

        self.assertEqual(lock_path.read_text(encoding="utf-8"), "")

    def test_queue_producer_lock_raises_with_existing_holder_when_busy(self):
        if self.mod.fcntl is None:
            self.skipTest("fcntl unavailable")

        lock_path = self.workspace_root / "data" / "locks" / "producer.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text(
            "pid=11111 host=holder-host started_at=2026-03-07T00:00:00+00:00\n",
            encoding="utf-8",
        )
        with mock.patch.object(self.mod.fcntl, "flock", side_effect=BlockingIOError()):
            with self.assertRaises(self.mod.ProducerLockAcquisitionError) as raised:
                with self.mod.queue_producer_lock(lock_path, enabled=True):
                    pass

        message = str(raised.exception)
        self.assertIn("producer lock busy:", message)
        self.assertIn("pid=11111", message)

    def test_enqueue_work_item_requeue_and_keep_success(self):
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            now_iso = "2026-03-07T00:00:00+00:00"

            action_1 = self.mod.enqueue_work_item(
                connection=connection,
                source_id="storiesofcz",
                stage="media",
                video_id="7611111111111111111",
                now_iso=now_iso,
                priority=10,
            )
            self.assertEqual(action_1, "inserted")

            action_2 = self.mod.enqueue_work_item(
                connection=connection,
                source_id="storiesofcz",
                stage="media",
                video_id="7611111111111111111",
                now_iso=now_iso,
                priority=5,
            )
            self.assertEqual(action_2, "updated")

            connection.execute(
                """
                UPDATE work_items
                SET status='error', next_retry_at='2026-03-08T00:00:00+00:00', last_error='x'
                WHERE source_id='storiesofcz' AND stage='media' AND video_id='7611111111111111111'
                """
            )
            action_3 = self.mod.enqueue_work_item(
                connection=connection,
                source_id="storiesofcz",
                stage="media",
                video_id="7611111111111111111",
                now_iso="2026-03-07T01:00:00+00:00",
                priority=8,
            )
            self.assertEqual(action_3, "requeued")

            row = connection.execute(
                """
                SELECT status, next_retry_at, last_error
                FROM work_items
                WHERE source_id='storiesofcz' AND stage='media' AND video_id='7611111111111111111'
                """
            ).fetchone()
            self.assertEqual(row[0], "queued")
            self.assertIsNone(row[1])
            self.assertIsNone(row[2])

            connection.execute(
                """
                UPDATE work_items
                SET status='success'
                WHERE source_id='storiesofcz' AND stage='media' AND video_id='7611111111111111111'
                """
            )
            action_4 = self.mod.enqueue_work_item(
                connection=connection,
                source_id="storiesofcz",
                stage="media",
                video_id="7611111111111111111",
                now_iso="2026-03-07T02:00:00+00:00",
                priority=1,
            )
            self.assertEqual(action_4, "kept")
        finally:
            connection.close()

    def test_enqueue_source_media_discovery_respects_poll_interval(self):
        source_root = self.workspace_root / "storiesofcz_queue"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"
media_discovery_interval_hours = 24

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)

            with mock.patch.object(
                self.mod,
                "discover_playlist_window_ids",
                return_value=["7611111111111111111", "7612222222222222222"],
            ) as discover_mock:
                discovered, inserted, _ = self.mod.enqueue_source_media_discovery(
                    connection=connection,
                    source=source,
                    dry_run=False,
                    run_label="sync-queue",
                    enforce_poll_interval=True,
                )
            self.assertEqual(discovered, 2)
            self.assertEqual(inserted, 2)
            self.assertEqual(discover_mock.call_count, 1)

            with mock.patch.object(
                self.mod,
                "discover_playlist_window_ids",
                return_value=["7613333333333333333"],
            ) as discover_mock_2:
                discovered_2, inserted_2, requeued_2 = self.mod.enqueue_source_media_discovery(
                    connection=connection,
                    source=source,
                    dry_run=False,
                    run_label="sync-queue",
                    enforce_poll_interval=True,
                )
            self.assertEqual(discovered_2, 0)
            self.assertEqual(inserted_2, 0)
            self.assertEqual(requeued_2, 0)
            self.assertEqual(discover_mock_2.call_count, 0)
        finally:
            connection.close()

    def test_lease_next_work_item_and_complete_success(self):
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            now_iso = "2026-03-07T00:00:00+00:00"
            self.mod.enqueue_work_item(
                connection=connection,
                source_id="storiesofcz",
                stage="meta",
                video_id="7611111111111111111",
                now_iso=now_iso,
                priority=5,
            )
            self.mod.enqueue_work_item(
                connection=connection,
                source_id="storiesofcz",
                stage="meta",
                video_id="7612222222222222222",
                now_iso=now_iso,
                priority=8,
            )
            connection.commit()

            leased = self.mod.lease_next_work_item(
                connection=connection,
                worker_id="worker-test",
                stages=["meta"],
                lease_seconds=600,
            )
            self.assertIsNotNone(leased)
            self.assertEqual(leased["video_id"], "7611111111111111111")

            completed = self.mod.complete_work_item_success(
                connection=connection,
                work_item_id=int(leased["id"]),
                lease_token=str(leased["lease_token"]),
                finished_at="2026-03-07T00:01:00+00:00",
            )
            self.assertTrue(completed)

            row = connection.execute(
                """
                SELECT status, attempt_count, lease_owner, lease_token
                FROM work_items
                WHERE id = ?
                """,
                (int(leased["id"]),),
            ).fetchone()
            self.assertEqual(row[0], "success")
            self.assertEqual(int(row[1]), 1)
            self.assertIsNone(row[2])
            self.assertIsNone(row[3])
        finally:
            connection.close()

    def test_fail_work_item_lease_schedules_retry_then_dead(self):
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id="storiesofcz",
                stage="meta",
                video_id="7613333333333333333",
                now_iso="2026-03-07T00:00:00+00:00",
                priority=1,
            )
            connection.commit()

            leased_1 = self.mod.lease_next_work_item(
                connection=connection,
                worker_id="worker-test",
                stages=["meta"],
                lease_seconds=600,
            )
            self.assertIsNotNone(leased_1)

            status_1, attempt_1, retry_1 = self.mod.fail_work_item_lease(
                connection=connection,
                work_item_id=int(leased_1["id"]),
                lease_token=str(leased_1["lease_token"]),
                error_message="simulated failure",
                max_attempts=2,
                finished_at="2026-03-07T00:02:00+00:00",
            )
            self.assertEqual(status_1, "error")
            self.assertEqual(attempt_1, 1)
            self.assertIsNotNone(retry_1)

            connection.execute(
                "UPDATE work_items SET next_retry_at = NULL WHERE id = ?",
                (int(leased_1["id"]),),
            )
            connection.commit()

            leased_2 = self.mod.lease_next_work_item(
                connection=connection,
                worker_id="worker-test",
                stages=["meta"],
                lease_seconds=600,
            )
            self.assertIsNotNone(leased_2)
            self.assertEqual(int(leased_1["id"]), int(leased_2["id"]))

            status_2, attempt_2, retry_2 = self.mod.fail_work_item_lease(
                connection=connection,
                work_item_id=int(leased_2["id"]),
                lease_token=str(leased_2["lease_token"]),
                error_message="simulated failure again",
                max_attempts=2,
                finished_at="2026-03-07T00:03:00+00:00",
            )
            self.assertEqual(status_2, "dead")
            self.assertEqual(attempt_2, 2)
            self.assertIsNone(retry_2)

            row = connection.execute(
                "SELECT status, attempt_count FROM work_items WHERE id = ?",
                (int(leased_2["id"]),),
            ).fetchone()
            self.assertEqual(row[0], "dead")
            self.assertEqual(int(row[1]), 2)
        finally:
            connection.close()

    def test_extend_work_item_lease_updates_expiry(self):
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id="storiesofcz",
                stage="media",
                video_id="7615555555555555555",
                now_iso="2026-03-07T00:00:00+00:00",
                priority=1,
            )
            connection.commit()

            leased = self.mod.lease_next_work_item(
                connection=connection,
                worker_id="worker-lease",
                stages=["media"],
                lease_seconds=120,
            )
            self.assertIsNotNone(leased)
            initial_expiry = str(leased["lease_expires_at"])
            connection.execute(
                "UPDATE work_items SET lease_expires_at = ? WHERE id = ?",
                ("2026-03-07T00:01:00+00:00", int(leased["id"])),
            )
            connection.commit()

            extended, next_expiry = self.mod.extend_work_item_lease(
                connection=connection,
                work_item_id=int(leased["id"]),
                lease_token=str(leased["lease_token"]),
                lease_seconds=600,
            )
            self.assertTrue(extended)
            self.assertIsNotNone(next_expiry)
            self.assertNotEqual(initial_expiry, str(next_expiry))

            row = connection.execute(
                "SELECT lease_expires_at FROM work_items WHERE id = ?",
                (int(leased["id"]),),
            ).fetchone()
            self.assertEqual(str(row[0]), str(next_expiry))
        finally:
            connection.close()

    def test_run_queue_worker_media_enqueues_subs_meta(self):
        source_root = self.workspace_root / "storiesofcz_queue_worker_media"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="media",
                video_id="7616666666666666666",
                now_iso="2026-03-07T00:00:00+00:00",
                priority=1,
            )
            connection.commit()
        finally:
            connection.close()

        def fake_sync_source(**kwargs):
            connection_for_sync = kwargs["connection"]
            target_source = kwargs["source"]
            target_video_id = str(kwargs["media_candidate_ids"][0])
            self.mod.upsert_download_state(
                connection=connection_for_sync,
                source_id=target_source.id,
                stage="media",
                video_id=target_video_id,
                status="success",
                run_id=None,
                attempt_at="2026-03-07T00:05:00+00:00",
                url=f"https://example.com/{target_video_id}",
                last_error=None,
                retry_count=0,
                next_retry_at=None,
            )
            connection_for_sync.commit()

        with mock.patch.object(self.mod, "sync_source", side_effect=fake_sync_source):
            self.mod.run_queue_worker(
                sources=[source],
                db_path=self.db_path,
                stages=["media"],
                worker_id="worker-downstream",
                lease_seconds=600,
                poll_interval_sec=0.2,
                max_items=1,
                once=False,
                dry_run=False,
                max_attempts=3,
                enqueue_downstream=True,
            )

        connection_check = sqlite3.connect(str(self.db_path))
        try:
            rows = connection_check.execute(
                """
                SELECT stage, status
                FROM work_items
                WHERE source_id = ? AND video_id = ?
                ORDER BY stage ASC
                """,
                (source.id, "7616666666666666666"),
            ).fetchall()
            stage_status = {str(stage): str(status) for stage, status in rows}
            self.assertEqual(stage_status.get("media"), "success")
            self.assertEqual(stage_status.get("subs"), "queued")
            self.assertEqual(stage_status.get("meta"), "queued")
            self.assertEqual(stage_status.get("loudness"), "queued")
        finally:
            connection_check.close()

    def test_run_queue_worker_media_enqueues_asr_when_enabled(self):
        source_root = self.workspace_root / "storiesofcz_queue_worker_media_asr"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
asr_enabled = true
asr_command = ["echo", "asr"]
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="media",
                video_id="7618888888888888888",
                now_iso="2026-03-07T00:00:00+00:00",
                priority=1,
            )
            connection.commit()
        finally:
            connection.close()

        def fake_sync_source(**kwargs):
            connection_for_sync = kwargs["connection"]
            target_source = kwargs["source"]
            target_video_id = str(kwargs["media_candidate_ids"][0])
            self.mod.upsert_download_state(
                connection=connection_for_sync,
                source_id=target_source.id,
                stage="media",
                video_id=target_video_id,
                status="success",
                run_id=None,
                attempt_at="2026-03-07T00:06:00+00:00",
                url=f"https://example.com/{target_video_id}",
                last_error=None,
                retry_count=0,
                next_retry_at=None,
            )
            connection_for_sync.commit()

        with mock.patch.object(self.mod, "sync_source", side_effect=fake_sync_source):
            self.mod.run_queue_worker(
                sources=[source],
                db_path=self.db_path,
                stages=["media"],
                worker_id="worker-downstream-asr",
                lease_seconds=600,
                poll_interval_sec=0.2,
                max_items=1,
                once=False,
                dry_run=False,
                max_attempts=3,
                enqueue_downstream=True,
            )

        connection_check = sqlite3.connect(str(self.db_path))
        try:
            rows = connection_check.execute(
                """
                SELECT stage, status
                FROM work_items
                WHERE source_id = ? AND video_id = ?
                ORDER BY stage ASC
                """,
                (source.id, "7618888888888888888"),
            ).fetchall()
            stage_status = {str(stage): str(status) for stage, status in rows}
            self.assertEqual(stage_status.get("media"), "success")
            self.assertEqual(stage_status.get("subs"), "queued")
            self.assertEqual(stage_status.get("meta"), "queued")
            self.assertEqual(stage_status.get("asr"), "queued")
            self.assertEqual(stage_status.get("loudness"), "queued")
        finally:
            connection_check.close()

    def test_run_queue_worker_processes_meta_item(self):
        source_root = self.workspace_root / "storiesofcz_queue_worker"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="meta",
                video_id="7614444444444444444",
                now_iso="2026-03-07T00:00:00+00:00",
                priority=1,
            )
            connection.commit()
        finally:
            connection.close()

        def fake_sync_source(**kwargs):
            connection_for_sync = kwargs["connection"]
            target_source = kwargs["source"]
            target_video_id = str(kwargs["metadata_candidate_ids"][0])
            self.mod.upsert_download_state(
                connection=connection_for_sync,
                source_id=target_source.id,
                stage="meta",
                video_id=target_video_id,
                status="success",
                run_id=None,
                attempt_at="2026-03-07T00:04:00+00:00",
                url=f"https://example.com/{target_video_id}",
                last_error=None,
                retry_count=0,
                next_retry_at=None,
            )
            connection_for_sync.commit()

        with mock.patch.object(self.mod, "sync_source", side_effect=fake_sync_source) as sync_mock:
            self.mod.run_queue_worker(
                sources=[source],
                db_path=self.db_path,
                stages=["meta"],
                worker_id="worker-ut",
                lease_seconds=600,
                poll_interval_sec=0.2,
                max_items=1,
                once=False,
                dry_run=False,
                max_attempts=3,
            )

        self.assertEqual(sync_mock.call_count, 1)
        self.assertTrue(bool(sync_mock.call_args.kwargs["strict_candidate_scope"]))
        self.assertEqual(sync_mock.call_args.kwargs["metadata_candidate_ids"], ["7614444444444444444"])

        connection_check = sqlite3.connect(str(self.db_path))
        try:
            row = connection_check.execute(
                """
                SELECT status, attempt_count, last_error
                FROM work_items
                WHERE source_id = ? AND stage = 'meta' AND video_id = ?
                """,
                (source.id, "7614444444444444444"),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "success")
            self.assertEqual(int(row[1]), 1)
            self.assertIn(str(row[2] or ""), {"", "None"})
        finally:
            connection_check.close()

    def test_run_queue_worker_processes_asr_item(self):
        source_root = self.workspace_root / "storiesofcz_queue_worker_asr"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
asr_enabled = true
asr_command = ["echo", "asr"]
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="asr",
                video_id="7619999999999999999",
                now_iso="2026-03-07T00:00:00+00:00",
                priority=1,
            )
            connection.commit()
        finally:
            connection.close()

        with mock.patch.object(self.mod, "run_asr_for_video", return_value=(True, None)) as asr_mock:
            self.mod.run_queue_worker(
                sources=[source],
                db_path=self.db_path,
                stages=["asr"],
                worker_id="worker-asr",
                lease_seconds=600,
                poll_interval_sec=0.2,
                max_items=1,
                once=False,
                dry_run=False,
                max_attempts=3,
                enqueue_downstream=False,
            )

        self.assertEqual(asr_mock.call_count, 1)
        self.assertEqual(str(asr_mock.call_args.kwargs["video_id"]), "7619999999999999999")

        connection_check = sqlite3.connect(str(self.db_path))
        try:
            row = connection_check.execute(
                """
                SELECT status
                FROM work_items
                WHERE source_id = ? AND stage = 'asr' AND video_id = ?
                """,
                (source.id, "7619999999999999999"),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "success")
        finally:
            connection_check.close()

    def test_run_queue_worker_asr_enqueues_translate(self):
        source_root = self.workspace_root / "storiesofcz_queue_worker_asr_downstream"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
asr_enabled = true
asr_command = ["echo", "asr"]
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="asr",
                video_id="7621111111111111111",
                now_iso="2026-03-07T00:00:00+00:00",
                priority=1,
            )
            connection.commit()
        finally:
            connection.close()

        with mock.patch.object(self.mod, "run_asr_for_video", return_value=(True, None)):
            self.mod.run_queue_worker(
                sources=[source],
                db_path=self.db_path,
                stages=["asr"],
                worker_id="worker-asr-downstream",
                lease_seconds=600,
                poll_interval_sec=0.2,
                max_items=1,
                once=False,
                dry_run=False,
                max_attempts=3,
                enqueue_downstream=True,
            )

        connection_check = sqlite3.connect(str(self.db_path))
        try:
            rows = connection_check.execute(
                """
                SELECT stage, status
                FROM work_items
                WHERE source_id = ? AND video_id = ?
                ORDER BY stage ASC
                """,
                (source.id, "7621111111111111111"),
            ).fetchall()
            stage_status = {str(stage): str(status) for stage, status in rows}
            self.assertEqual(stage_status.get("asr"), "success")
            self.assertEqual(stage_status.get("translate"), "queued")
        finally:
            connection_check.close()

    def test_run_queue_worker_subs_enqueues_translate(self):
        source_root = self.workspace_root / "storiesofcz_queue_worker_subs_downstream"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="subs",
                video_id="7622222222222222222",
                now_iso="2026-03-07T00:00:00+00:00",
                priority=1,
            )
            connection.commit()
        finally:
            connection.close()

        def fake_sync_source(**kwargs):
            connection_for_sync = kwargs["connection"]
            target_source = kwargs["source"]
            target_video_id = str(kwargs["metadata_candidate_ids"][0])
            self.mod.upsert_download_state(
                connection=connection_for_sync,
                source_id=target_source.id,
                stage="subs",
                video_id=target_video_id,
                status="success",
                run_id=None,
                attempt_at="2026-03-07T00:09:00+00:00",
                url=f"https://example.com/{target_video_id}",
                last_error=None,
                retry_count=0,
                next_retry_at=None,
            )
            connection_for_sync.commit()

        with mock.patch.object(self.mod, "sync_source", side_effect=fake_sync_source):
            self.mod.run_queue_worker(
                sources=[source],
                db_path=self.db_path,
                stages=["subs"],
                worker_id="worker-subs-downstream",
                lease_seconds=600,
                poll_interval_sec=0.2,
                max_items=1,
                once=False,
                dry_run=False,
                max_attempts=3,
                enqueue_downstream=True,
            )

        connection_check = sqlite3.connect(str(self.db_path))
        try:
            rows = connection_check.execute(
                """
                SELECT stage, status
                FROM work_items
                WHERE source_id = ? AND video_id = ?
                ORDER BY stage ASC
                """,
                (source.id, "7622222222222222222"),
            ).fetchall()
            stage_status = {str(stage): str(status) for stage, status in rows}
            self.assertEqual(stage_status.get("subs"), "success")
            self.assertEqual(stage_status.get("translate"), "queued")
        finally:
            connection_check.close()

    def test_run_queue_worker_processes_loudness_item(self):
        source_root = self.workspace_root / "storiesofcz_queue_worker_loudness"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="loudness",
                video_id="7620000000000000000",
                now_iso="2026-03-07T00:00:00+00:00",
                priority=1,
            )
            connection.commit()
        finally:
            connection.close()

        with mock.patch.object(self.mod, "run_loudness_for_video", return_value=(True, None)) as loud_mock:
            self.mod.run_queue_worker(
                sources=[source],
                db_path=self.db_path,
                stages=["loudness"],
                worker_id="worker-loudness",
                lease_seconds=600,
                poll_interval_sec=0.2,
                max_items=1,
                once=False,
                dry_run=False,
                max_attempts=3,
                enqueue_downstream=False,
            )

        self.assertEqual(loud_mock.call_count, 1)
        self.assertEqual(str(loud_mock.call_args.kwargs["video_id"]), "7620000000000000000")

        connection_check = sqlite3.connect(str(self.db_path))
        try:
            row = connection_check.execute(
                """
                SELECT status
                FROM work_items
                WHERE source_id = ? AND stage = 'loudness' AND video_id = ?
                """,
                (source.id, "7620000000000000000"),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "success")
        finally:
            connection_check.close()

    def test_run_queue_worker_processes_translate_item(self):
        source_root = self.workspace_root / "storiesofcz_queue_worker_translate"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="translate",
                video_id="7623333333333333333",
                now_iso="2026-03-07T00:00:00+00:00",
                priority=1,
            )
            connection.commit()
        finally:
            connection.close()

        with mock.patch.object(
            self.mod,
            "run_translate_local_for_video",
            return_value=(True, None),
        ) as translate_mock:
            self.mod.run_queue_worker(
                sources=[source],
                db_path=self.db_path,
                stages=["translate"],
                worker_id="worker-translate",
                lease_seconds=600,
                poll_interval_sec=0.2,
                max_items=1,
                once=False,
                dry_run=False,
                max_attempts=3,
                enqueue_downstream=False,
                translate_target_lang="ja-local-custom",
                translate_source_track="asr",
                translate_timeout_sec=123,
            )

        self.assertEqual(translate_mock.call_count, 1)
        self.assertEqual(str(translate_mock.call_args.kwargs["video_id"]), "7623333333333333333")
        self.assertEqual(str(translate_mock.call_args.kwargs["target_lang"]), "ja-local-custom")
        self.assertEqual(str(translate_mock.call_args.kwargs["source_track"]), "asr")
        self.assertEqual(int(translate_mock.call_args.kwargs["timeout_sec"]), 123)

        connection_check = sqlite3.connect(str(self.db_path))
        try:
            row = connection_check.execute(
                """
                SELECT status
                FROM work_items
                WHERE source_id = ? AND stage = 'translate' AND video_id = ?
                """,
                (source.id, "7623333333333333333"),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "success")
        finally:
            connection_check.close()

    def test_sync_source_uses_run_local_urls_file_by_default(self):
        source_root = self.workspace_root / "storiesofcz_urls_local"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)

            written_paths: list[Path] = []

            def fake_write_urls(path, urls):
                written_paths.append(Path(path))

            with mock.patch.object(self.mod, "write_urls_file", side_effect=fake_write_urls):
                with mock.patch.object(self.mod, "run_command", return_value=0):
                    self.mod.sync_source(
                        source=source,
                        dry_run=False,
                        skip_media=True,
                        skip_subs=True,
                        skip_meta=False,
                        connection=connection,
                        metadata_candidate_ids=["7617777777777777777"],
                    )

            self.assertEqual(len(written_paths), 1)
            active_path = written_paths[0]
            self.assertNotEqual(active_path, source.urls_file)
            self.assertEqual(active_path.parent.name, "tmp")
            self.assertTrue(active_path.name.startswith("urls."))
            self.assertTrue(active_path.name.endswith(".txt"))
        finally:
            connection.close()

    def test_sync_source_defers_media_discovery_with_recent_attempt(self):
        source_root = self.workspace_root / "storiesofcz"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"
media_discovery_interval_hours = 24

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            recent_attempt_iso = self.mod.now_utc_iso()
            self.mod.set_app_state_value(
                connection=connection,
                state_key=f"media_discovery_last_attempt:{source.id}",
                state_value=recent_attempt_iso,
            )
            connection.commit()

            with mock.patch.object(self.mod, "run_command") as run_command_mock:
                self.mod.sync_source(
                    source=source,
                    dry_run=False,
                    skip_media=False,
                    skip_subs=True,
                    skip_meta=True,
                    connection=connection,
                )
            run_command_mock.assert_not_called()

            media_run = connection.execute(
                """
                SELECT status, command, success_count, failure_count
                FROM download_runs
                WHERE source_id = ? AND stage = 'media'
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (source.id,),
            ).fetchone()
            self.assertIsNotNone(media_run)
            self.assertEqual(media_run[0], "success")
            self.assertIsNone(media_run[1])
            self.assertEqual(int(media_run[2] or 0), 0)
            self.assertEqual(int(media_run[3] or 0), 0)

            saved_attempt = self.mod.get_app_state_value(
                connection,
                f"media_discovery_last_attempt:{source.id}",
                default="",
            )
            self.assertEqual(saved_attempt, recent_attempt_iso)
        finally:
            connection.close()

    def test_sync_source_metered_updates_only_skips_unseeded_media(self):
        source_root = self.workspace_root / "storiesofcz_metered_skip"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            with mock.patch.object(self.mod, "run_command") as run_command_mock:
                self.mod.sync_source(
                    source=source,
                    dry_run=False,
                    skip_media=False,
                    skip_subs=True,
                    skip_meta=True,
                    connection=connection,
                    metered_media_mode="updates-only",
                    metered_min_archive_ids=1,
                    metered_playlist_end=5,
                )
            run_command_mock.assert_not_called()

            media_run = connection.execute(
                """
                SELECT run_id
                FROM download_runs
                WHERE source_id = ? AND stage = 'media'
                LIMIT 1
                """,
                (source.id,),
            ).fetchone()
            self.assertIsNone(media_run)
        finally:
            connection.close()

    def test_sync_source_metered_updates_only_limits_discovery_command(self):
        source_root = self.workspace_root / "storiesofcz_metered_limit"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "storiesofcz"
platform = "tiktok"
url = "https://www.tiktok.com/@storiesofcz"
enabled = true
data_dir = "{source_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]
        source.media_archive.parent.mkdir(parents=True, exist_ok=True)
        source.media_archive.write_text(
            "tiktok 7611234567890123456\n",
            encoding="utf-8",
        )

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            with mock.patch.object(self.mod, "run_command", return_value=0) as run_command_mock:
                self.mod.sync_source(
                    source=source,
                    dry_run=False,
                    skip_media=False,
                    skip_subs=True,
                    skip_meta=True,
                    connection=connection,
                    metered_media_mode="updates-only",
                    metered_min_archive_ids=1,
                    metered_playlist_end=5,
                )

            self.assertGreaterEqual(run_command_mock.call_count, 1)
            media_command = list(run_command_mock.call_args_list[0].args[0])
            self.assertIn("--break-on-existing", media_command)
            self.assertIn("--playlist-end", media_command)
            playlist_end_index = media_command.index("--playlist-end")
            self.assertEqual(str(media_command[playlist_end_index + 1]), "5")
        finally:
            connection.close()

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
