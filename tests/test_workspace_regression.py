import importlib.util
import io
import json
import sqlite3
import sys
import tempfile
import threading
import unittest
import datetime as dt
from contextlib import redirect_stderr, redirect_stdout
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

    def test_build_translation_output_origin_lookup_accepts_tuple_rows(self):
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            source_path = self.workspace_root / "source.vtt"
            output_path = self.workspace_root / "output.ja-local.vtt"
            source_path.write_text("WEBVTT\n", encoding="utf-8")
            output_path.write_text("WEBVTT\n", encoding="utf-8")
            self.mod.record_translation_run(
                connection=connection,
                source_id="storiesofcz",
                video_id="7619999999999999999",
                source_path=source_path,
                output_path=output_path,
                cue_count=1,
                cue_match=True,
                agent="local-llm",
                method="multi-stage",
                method_version="source-track=subtitle",
                summary="ok",
                status="success",
            )
            connection.commit()

            lookup = self.mod.build_translation_output_origin_lookup(
                connection=connection,
                source_id="storiesofcz",
                video_id="7619999999999999999",
            )
        finally:
            connection.close()

        self.assertEqual(
            lookup,
            {str(output_path): ("generated", "translate-local")},
        )

    def test_subtitle_language_matches_sub_langs_accepts_prefixed_labels(self):
        self.assertTrue(
            self.mod.subtitle_language_matches_sub_langs("NA.jpn-JP", "ja.*,ja,jp.*,jpn.*")
        )
        self.assertTrue(
            self.mod.subtitle_language_matches_sub_langs("NA.eng-US", "en.*,en")
        )

    def test_classify_ja_subtitle_variant_accepts_upstream_japanese_codes(self):
        self.assertEqual(
            self.mod.classify_ja_subtitle_variant("NA.jpn-JP"),
            "upstream",
        )
        self.assertEqual(
            self.mod.classify_ja_subtitle_variant("JP.ja-JP"),
            "upstream",
        )
        self.assertEqual(
            self.mod.classify_ja_subtitle_variant("ja-local"),
            "local",
        )
        self.assertEqual(
            self.mod.classify_ja_subtitle_variant("ja-asr-local"),
            "local",
        )
        self.assertEqual(
            self.mod.classify_ja_subtitle_variant(
                "ja",
                origin_kind="generated",
                origin_detail="generated:claude-opus-4-6",
            ),
            "claude",
        )
        self.assertEqual(
            self.mod.classify_ja_subtitle_variant(
                "ja",
                origin_kind="generated",
                origin_detail="translate-local",
            ),
            "local",
        )

    def test_run_command_with_output_streams_and_returns_combined_text(self):
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            exit_code, output_text = self.mod.run_command_with_output(
                [
                    sys.executable,
                    "-c",
                    (
                        "import sys, time; "
                        "print('out-1', flush=True); "
                        "print('err-1', file=sys.stderr, flush=True); "
                        "time.sleep(0.05); "
                        "print('out-2', flush=True); "
                        "print('err-2', file=sys.stderr, flush=True)"
                    ),
                ],
                dry_run=False,
            )

        self.assertEqual(exit_code, 0)
        self.assertIn("out-1", stdout_capture.getvalue())
        self.assertIn("out-2", stdout_capture.getvalue())
        self.assertIn("err-1", stderr_capture.getvalue())
        self.assertIn("err-2", stderr_capture.getvalue())
        self.assertIn("out-1", output_text)
        self.assertIn("out-2", output_text)
        self.assertIn("err-1", output_text)
        self.assertIn("err-2", output_text)

    def test_run_command_with_output_interrupt_terminates_child_process_group(self):
        class FakeProcess:
            def __init__(self):
                self.stdout = io.StringIO("")
                self.stderr = io.StringIO("")
                self.pid = 4242
                self.returncode = None
                self.wait_calls: list[float | None] = []

            def wait(self, timeout=None):
                self.wait_calls.append(timeout)
                if timeout is None:
                    raise KeyboardInterrupt()
                self.returncode = -15
                return self.returncode

            def poll(self):
                return self.returncode

            def terminate(self):
                self.returncode = -15

            def kill(self):
                self.returncode = -9

        fake_process = FakeProcess()

        def fake_killpg(_pgid, _sig):
            fake_process.returncode = -15

        with (
            mock.patch.object(self.mod.subprocess, "Popen", return_value=fake_process) as popen_mock,
            mock.patch.object(self.mod.os, "getpgid", return_value=fake_process.pid),
            mock.patch.object(self.mod.os, "killpg", side_effect=fake_killpg) as killpg_mock,
        ):
            with self.assertRaises(KeyboardInterrupt):
                self.mod.run_command_with_output(["fake-ytdlp", "--version"], dry_run=False)

        self.assertTrue(fake_process.stdout.closed)
        self.assertTrue(fake_process.stderr.closed)
        self.assertEqual(fake_process.wait_calls, [None, 2.0])
        self.assertEqual(killpg_mock.call_count, 1)
        popen_kwargs = popen_mock.call_args.kwargs
        self.assertTrue(popen_kwargs["start_new_session"])

    def test_run_interleaved_chunked_ytdlp_stage_plans_retries_transient_tiktok_errors(self):
        source_root = self.workspace_root / "transient_retry_source"
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
        active_urls_file = source.media_archive.parent / "tmp" / "retry_urls.txt"
        active_urls_file.parent.mkdir(parents=True, exist_ok=True)
        observed_batches: list[list[str]] = []

        def fake_run_command(command, dry_run=False):
            self.assertFalse(dry_run)
            args = list(command)
            urls_path = Path(args[args.index("-a") + 1])
            batch_ids = [url.rstrip("/").rsplit("/", 1)[-1] for url in urls_path.read_text(encoding="utf-8").splitlines()]
            observed_batches.append(batch_ids)
            if len(observed_batches) == 1:
                return (
                    1,
                    "ERROR: [TikTok] 7437177985746226487: Unable to extract universal data for rehydration",
                )
            return (0, "")

        plan = self.mod.ChunkedYtdlpStagePlan(
            source=source,
            stage="subs",
            active_urls_file=active_urls_file,
            build_command=lambda: ["fake-ytdlp"],
            command_template=["fake-ytdlp"],
            url_chunks=[[
                ("7437177985746226487", "https://www.tiktok.com/@storiesofcz/video/7437177985746226487"),
                ("7436457164912299319", "https://www.tiktok.com/@storiesofcz/video/7436457164912299319"),
            ]],
            target_ids=["7437177985746226487", "7436457164912299319"],
            resolved_target_ids=["7437177985746226487", "7436457164912299319"],
            unresolved_target_ids=[],
            started_at="2026-03-10T00:00:00+00:00",
            run_id=None,
        )

        with mock.patch.object(self.mod, "run_command_with_output", side_effect=fake_run_command):
            self.mod.run_interleaved_chunked_ytdlp_stage_plans([plan], dry_run=False)

        self.assertEqual(
            observed_batches,
            [
                ["7437177985746226487", "7436457164912299319"],
                ["7437177985746226487"],
            ],
        )
        self.assertEqual(plan.chunk_index, 1)
        self.assertIsNone(plan.error)

    def test_extract_requested_subtitles_unavailable_video_ids_tracks_current_video(self):
        candidate_ids = [
            "7579312035477916958",
            "7579105528832396574",
        ]
        output_text = """
[TikTok] Extracting URL: https://www.tiktok.com/@alexisanddean/video/7579312035477916958
[TikTok] 7579312035477916958: Downloading webpage
[info] 7579312035477916958: Downloading 1 format(s): bytevc1_1080p_846241-1
[info] There are no subtitles for the requested languages
[TikTok] Extracting URL: https://www.tiktok.com/@alexisanddean/video/7579105528832396574
[TikTok] 7579105528832396574: Downloading webpage
[info] 7579105528832396574: Downloading 1 format(s): bytevc1_1080p_891343-1
[info] There are no subtitles for the requested languages
[info] There are no subtitles for the requested languages
        """.strip()

        self.assertEqual(
            self.mod.extract_requested_subtitles_unavailable_video_ids(
                output_text,
                candidate_ids,
            ),
            candidate_ids,
        )

    def test_finalize_subtitle_download_plan_records_requested_subtitles_unavailable(self):
        source_root = self.workspace_root / "requested_subtitles_unavailable_source"
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
id = "alexisanddean"
platform = "tiktok"
url = "https://www.tiktok.com/@alexisanddean"
enabled = true
data_dir = "{source_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = sources[0]
        active_urls_file = source.media_archive.parent / "tmp" / "requested_subtitles_unavailable.txt"
        active_urls_file.parent.mkdir(parents=True, exist_ok=True)
        video_id = "7579312035477916958"
        video_url = self.mod.build_video_url(source, video_id)

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            connection.execute(
                """
                INSERT INTO videos(source_id, video_id, media_path, has_media, has_subtitles, synced_at)
                VALUES (?, ?, ?, 1, 0, ?)
                """,
                (
                    source.id,
                    video_id,
                    str(source.media_dir / f"{video_id}.mp4"),
                    "2026-03-10T00:00:00+00:00",
                ),
            )
            connection.commit()

            plan = self.mod.ChunkedYtdlpStagePlan(
                source=source,
                stage="subs",
                active_urls_file=active_urls_file,
                build_command=lambda: ["fake-ytdlp"],
                command_template=["fake-ytdlp"],
                url_chunks=[[(video_id, video_url)]],
                target_ids=[video_id],
                resolved_target_ids=[video_id],
                unresolved_target_ids=[],
                started_at="2026-03-10T00:00:00+00:00",
                run_id=None,
            )

            with mock.patch.object(
                self.mod,
                "run_command_with_output",
                return_value=(
                    0,
                    "\n".join(
                        [
                            f"[TikTok] Extracting URL: {video_url}",
                            f"[TikTok] {video_id}: Downloading webpage",
                            f"[info] {video_id}: Downloading 1 format(s): bytevc1_1080p_846241-1",
                            "[info] There are no subtitles for the requested languages",
                        ]
                    ),
                ),
            ):
                self.mod.run_interleaved_chunked_ytdlp_stage_plans([plan], dry_run=False)

            self.assertEqual(
                plan.payload.get("requested_subtitles_unavailable_ids"),
                [video_id],
            )

            self.mod.finalize_subtitle_download_plan(
                plan,
                dry_run=False,
                connection=connection,
                safe_video_url=lambda current_video_id: self.mod.build_video_url(source, current_video_id),
            )

            row = connection.execute(
                """
                SELECT status, retry_count, last_error, next_retry_at
                FROM download_state
                WHERE source_id = ? AND stage = 'subs' AND video_id = ?
                """,
                (source.id, video_id),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "error")
            self.assertEqual(int(row[1]), 1)
            self.assertEqual(
                str(row[2]),
                "There are no subtitles for the requested languages",
            )
            self.assertTrue(str(row[3] or "").strip())
        finally:
            connection.close()

    def test_finalize_subtitle_download_plan_records_transient_webpage_error_when_exit_zero(self):
        source_root = self.workspace_root / "requested_subtitles_transient_source"
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
id = "summer_exe"
platform = "tiktok"
url = "https://www.tiktok.com/@summer_exe"
enabled = true
data_dir = "{source_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        source = self.mod.apply_upstream_sub_langs_override(
            sources,
            "ja.*,ja,jp.*,jpn.*",
        )[0]
        video_id = "7558055850082848018"

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            source.media_dir.mkdir(parents=True, exist_ok=True)
            source.media_archive.parent.mkdir(parents=True, exist_ok=True)
            source.media_archive.write_text(
                f"tiktok {video_id}\n",
                encoding="utf-8",
            )
            connection.execute(
                """
                INSERT INTO videos(source_id, video_id, media_path, has_media, has_subtitles, synced_at)
                VALUES (?, ?, ?, 1, 0, ?)
                """,
                (
                    source.id,
                    video_id,
                    str(source.media_dir / f"{video_id}.mp4"),
                    "2026-03-10T00:00:00+00:00",
                ),
            )
            connection.commit()

            commands: list[list[str]] = []

            def fake_run_command_with_output(command, dry_run):
                commands.append(list(command))
                return (
                    0,
                    (
                        f"ERROR: [TikTok] {video_id}: "
                        "Unable to extract universal data for rehydration"
                    ),
                )

            with mock.patch.object(
                self.mod,
                "run_command_with_output",
                side_effect=fake_run_command_with_output,
            ):
                self.mod.sync_source(
                    source=source,
                    dry_run=False,
                    skip_media=True,
                    skip_subs=False,
                    skip_meta=True,
                    connection=connection,
                )

            self.assertEqual(len(commands), 2)
            row = connection.execute(
                """
                SELECT status, last_error
                FROM download_state
                WHERE source_id = ? AND stage = 'subs' AND video_id = ?
                """,
                (source.id, video_id),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "error")
            self.assertIn("rehydration", str(row[1]).lower())
            self.assertNotIn("missing after download attempt", str(row[1]).lower())
        finally:
            connection.close()

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

    def test_collect_workspace_source_processing_summary(self):
        now_iso = dt.datetime(2026, 3, 9, 1, 2, tzinfo=dt.timezone.utc).isoformat()
        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        try:
            connection.executemany(
                """
                INSERT INTO sources(source_id, platform, url, data_dir, updated_at)
                VALUES (?, 'tiktok', ?, ?, ?)
                """,
                [
                    ("alpha", "https://example.com/@alpha", "alpha", now_iso),
                    ("beta", "https://example.com/@beta", "beta", now_iso),
                ],
            )
            connection.executemany(
                """
                INSERT INTO videos(
                    source_id,
                    video_id,
                    meta_path,
                    has_media,
                    audio_loudness_analyzed_at,
                    synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    ("alpha", "complete", "/tmp/complete.info.json", 1, now_iso, now_iso),
                    ("alpha", "eng-missing", "/tmp/eng-missing.info.json", 1, "", now_iso),
                    ("alpha", "ja-missing", "/tmp/ja-missing.info.json", 1, "", now_iso),
                    ("alpha", "loudness-pending", "/tmp/loudness.info.json", 1, "", now_iso),
                    ("alpha", "media-missing", "", 0, "", now_iso),
                    ("beta", "asr-only", "/tmp/asr-only.info.json", 1, "", now_iso),
                ],
            )
            connection.executemany(
                """
                INSERT INTO subtitles(
                    source_id,
                    video_id,
                    language,
                    subtitle_path,
                    origin_kind,
                    origin_detail,
                    ext
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    ("alpha", "complete", "eng-US", "/tmp/complete.eng-US.vtt", "upstream", "", "vtt"),
                    ("alpha", "complete", "ja", "/tmp/complete.ja.vtt", "upstream", "", "vtt"),
                    ("alpha", "ja-missing", "en", "/tmp/ja-missing.en.vtt", "upstream", "", "vtt"),
                    ("alpha", "loudness-pending", "en", "/tmp/loudness.en.vtt", "upstream", "", "vtt"),
                    (
                        "alpha",
                        "loudness-pending",
                        "ja",
                        "/tmp/loudness.ja.vtt",
                        "generated",
                        "generated:claude",
                        "vtt",
                    ),
                ],
            )
            connection.execute(
                """
                INSERT INTO asr_runs(source_id, video_id, status, updated_at)
                VALUES (?, ?, 'success', ?)
                """,
                ("beta", "asr-only", now_iso),
            )
            connection.executemany(
                """
                INSERT INTO video_playback_stats(
                    source_id,
                    video_id,
                    impression_count,
                    play_count,
                    total_watch_seconds,
                    completed_count,
                    fast_skip_count,
                    shallow_skip_count,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
                """,
                [
                    ("alpha", "complete", 1, 1, 12.5, 0, now_iso, now_iso),
                    ("beta", "asr-only", 1, 1, 7.0, 0, now_iso, now_iso),
                ],
            )
            connection.commit()

            summary = self.mod.collect_workspace_source_processing_summary(
                connection=connection,
                source_ids=[],
            )
        finally:
            connection.close()

        self.assertEqual(summary["totals"]["source_count"], 2)
        self.assertEqual(summary["totals"]["total_videos"], 6)
        self.assertEqual(summary["totals"]["complete_count"], 1)
        self.assertEqual(summary["totals"]["played_videos"], 2)
        self.assertEqual(summary["totals"]["played_playable"], 2)
        self.assertEqual(summary["totals"]["subtitle_tracks_ready"], 3)
        self.assertEqual(summary["totals"]["subtitle_tracks_ready_playable"], 3)
        self.assertEqual(summary["totals"]["upstream_ja_subtitles_ready"], 1)
        self.assertEqual(summary["totals"]["upstream_ja_subtitles_ready_playable"], 1)

        by_source = {row["source_id"]: row for row in summary["sources"]}
        alpha = by_source["alpha"]
        self.assertEqual(alpha["total_videos"], 5)
        self.assertEqual(alpha["english_subtitles_missing"], 2)
        self.assertEqual(alpha["played_videos"], 1)
        self.assertEqual(alpha["played_playable"], 1)
        self.assertEqual(alpha["subtitle_tracks_ready"], 3)
        self.assertEqual(alpha["subtitle_tracks_ready_playable"], 3)
        self.assertEqual(alpha["subtitle_tracks_missing_playable"], 1)
        self.assertEqual(alpha["ja_subtitles_ready"], 2)
        self.assertEqual(alpha["ja_subtitles_ready_playable"], 2)
        self.assertEqual(alpha["upstream_ja_subtitles_ready"], 1)
        self.assertEqual(alpha["upstream_ja_subtitles_ready_playable"], 1)
        self.assertEqual(alpha["upstream_ja_subtitles_missing_playable"], 3)
        self.assertEqual(alpha["ja_subtitles_missing_playable"], 2)
        self.assertEqual(alpha["loudness_pending"], 3)
        self.assertEqual(alpha["meta_missing"], 1)
        self.assertEqual(alpha["media_missing"], 1)
        self.assertEqual(alpha["complete_count"], 1)
        self.assertEqual(alpha["pipeline_buckets"]["complete"], 1)
        self.assertEqual(alpha["pipeline_buckets"]["source_text_pending"], 1)
        self.assertEqual(alpha["pipeline_buckets"]["ja_pending"], 1)
        self.assertEqual(alpha["pipeline_buckets"]["loudness_pending"], 1)
        self.assertEqual(alpha["pipeline_buckets"]["meta_media_pending"], 1)

        beta = by_source["beta"]
        self.assertEqual(beta["total_videos"], 1)
        self.assertEqual(beta["english_subtitles_missing"], 1)
        self.assertEqual(beta["source_text_ready"], 1)
        self.assertEqual(beta["asr_ready"], 1)
        self.assertEqual(beta["played_videos"], 1)
        self.assertEqual(beta["played_playable"], 1)
        self.assertEqual(beta["subtitle_tracks_ready"], 0)
        self.assertEqual(beta["subtitle_tracks_ready_playable"], 0)
        self.assertEqual(beta["ja_subtitles_ready"], 0)
        self.assertEqual(beta["upstream_ja_subtitles_ready"], 0)
        self.assertEqual(beta["ja_subtitles_ready_playable"], 0)
        self.assertEqual(beta["upstream_ja_subtitles_ready_playable"], 0)
        self.assertEqual(beta["ja_subtitles_missing_playable"], 1)
        self.assertEqual(beta["pipeline_buckets"]["ja_pending"], 1)

    def test_collect_workspace_source_processing_trend_carries_forward_snapshots(self):
        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        try:
            self.mod.upsert_workspace_source_daily_metrics(
                connection,
                {
                    "sources": [
                        {
                            "source_id": "alpha",
                            "total_videos": 5,
                            "media_ready": 3,
                            "played_playable": 1,
                            "subtitle_tracks_ready_playable": 2,
                            "ja_subtitles_ready_playable": 1,
                            "upstream_ja_subtitles_ready_playable": 1,
                            "complete_count": 1,
                        },
                        {
                            "source_id": "beta",
                            "total_videos": 2,
                            "media_ready": 1,
                            "played_playable": 0,
                            "subtitle_tracks_ready_playable": 0,
                            "ja_subtitles_ready_playable": 0,
                            "upstream_ja_subtitles_ready_playable": 0,
                            "complete_count": 0,
                        },
                    ],
                },
                snapshot_at=dt.datetime(2026, 3, 10, 1, 0, tzinfo=dt.timezone.utc),
            )
            self.mod.upsert_workspace_source_daily_metrics(
                connection,
                {
                    "sources": [
                        {
                            "source_id": "alpha",
                            "total_videos": 6,
                            "media_ready": 4,
                            "played_playable": 2,
                            "subtitle_tracks_ready_playable": 3,
                            "ja_subtitles_ready_playable": 2,
                            "upstream_ja_subtitles_ready_playable": 1,
                            "complete_count": 2,
                        },
                        {
                            "source_id": "beta",
                            "total_videos": 3,
                            "media_ready": 2,
                            "played_playable": 1,
                            "subtitle_tracks_ready_playable": 1,
                            "ja_subtitles_ready_playable": 1,
                            "upstream_ja_subtitles_ready_playable": 0,
                            "complete_count": 1,
                        },
                    ],
                },
                snapshot_at=dt.datetime(2026, 3, 12, 1, 0, tzinfo=dt.timezone.utc),
            )
            connection.commit()

            trend = self.mod.collect_workspace_source_processing_trend(
                connection=connection,
                source_ids=[],
                window_days=7,
                end_date=dt.date(2026, 3, 12),
            )
        finally:
            connection.close()

        self.assertEqual(trend["window_days"], 7)
        self.assertEqual(trend["snapshot_days"], 2)
        self.assertEqual(len(trend["points"]), 7)
        points_by_date = {point["date"]: point for point in trend["points"]}
        self.assertEqual(points_by_date["2026-03-10"]["media_ready"], 4)
        self.assertEqual(points_by_date["2026-03-11"]["media_ready"], 4)
        self.assertEqual(points_by_date["2026-03-11"]["delta_media_ready"], 0)
        self.assertEqual(points_by_date["2026-03-12"]["media_ready"], 6)
        self.assertEqual(points_by_date["2026-03-12"]["delta_media_ready"], 2)
        self.assertEqual(points_by_date["2026-03-12"]["ja_subtitles_ready_playable"], 3)
        self.assertEqual(points_by_date["2026-03-12"]["played_playable"], 3)
        self.assertEqual(trend["latest"]["upstream_ja_subtitles_ready_playable"], 1)
        self.assertEqual(trend["net_change"]["media_ready"], 2)

    def test_collect_workspace_download_monitor_classifies_errors(self):
        now_dt = dt.datetime(2026, 3, 12, 2, 0, tzinfo=dt.timezone.utc)
        now_iso = now_dt.isoformat()
        recent_iso = (now_dt - dt.timedelta(hours=2)).isoformat()
        older_iso = (now_dt - dt.timedelta(hours=4)).isoformat()

        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        try:
            self.mod.create_schema(connection)
            connection.executemany(
                """
                INSERT INTO download_state(
                    source_id,
                    stage,
                    video_id,
                    status,
                    retry_count,
                    last_error,
                    last_attempt_at,
                    next_retry_at,
                    updated_at
                )
                VALUES (?, ?, ?, 'error', ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "alpha",
                        "subs",
                        "a1",
                        1,
                        "ERROR: [TikTok] a1: Unable to extract universal data for rehydration",
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                    (
                        "alpha",
                        "subs",
                        "a2",
                        2,
                        "ERROR: [TikTok] a2: Your IP address is blocked from accessing this post",
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                    (
                        "beta",
                        "subs",
                        "b1",
                        1,
                        "There are no subtitles for the requested languages",
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                    (
                        "beta",
                        "meta",
                        "b2",
                        1,
                        "metadata file missing after download attempt",
                        now_iso,
                        now_iso,
                        now_iso,
                    ),
                ],
            )
            connection.executemany(
                """
                INSERT INTO download_runs(
                    source_id,
                    stage,
                    status,
                    started_at,
                    finished_at,
                    exit_code,
                    target_count,
                    success_count,
                    failure_count,
                    error_message
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        "alpha",
                        "subs",
                        "error",
                        recent_iso,
                        recent_iso,
                        0,
                        5,
                        0,
                        5,
                        "ERROR: [TikTok] Unable to extract universal data for rehydration",
                    ),
                    (
                        "beta",
                        "subs",
                        "error",
                        older_iso,
                        older_iso,
                        0,
                        5,
                        2,
                        3,
                        "ERROR: [TikTok] 7451812681822080264: Your IP address is blocked from accessing this post",
                    ),
                ],
            )
            connection.commit()

            with mock.patch.object(
                self.mod.dt,
                "datetime",
                mock.Mock(wraps=dt.datetime, now=mock.Mock(return_value=now_dt)),
            ):
                monitor = self.mod.collect_workspace_download_monitor(
                    connection=connection,
                    source_ids=[],
                    since_hours=72,
                    run_limit=10,
                    pending_limit=10,
                )
        finally:
            connection.close()

        self.assertEqual(monitor["pending_count"], 4)
        self.assertEqual(monitor["pending_category_counts"]["transient"], 1)
        self.assertEqual(monitor["pending_category_counts"]["blocked"], 1)
        self.assertEqual(monitor["pending_category_counts"]["no_subtitles"], 1)
        self.assertEqual(monitor["pending_category_counts"]["missing_artifact"], 1)
        self.assertEqual(monitor["recent_error_runs"], 2)
        self.assertEqual(monitor["recent_runs"][0]["error_category"], "transient")
        pending_by_key = {
            (item["source_id"], item["video_id"]): item
            for item in monitor["pending_failures"]
        }
        self.assertEqual(pending_by_key[("alpha", "a2")]["error_category"], "blocked")
        self.assertEqual(monitor["per_source"]["alpha"]["pending_count"], 2)
        self.assertEqual(monitor["per_source"]["beta"]["recent_error_runs"], 1)

    def test_collect_workspace_download_error_trend_carries_forward_snapshots(self):
        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        try:
            self.mod.create_schema(connection)
            self.mod.upsert_workspace_download_daily_metrics(
                connection=connection,
                source_ids=["alpha", "beta"],
                download_monitor={
                    "per_source": {
                        "alpha": {
                            "source_id": "alpha",
                            "pending_count": 3,
                            "blocked_count": 1,
                            "transient_count": 2,
                            "no_subtitles_count": 0,
                            "missing_artifact_count": 0,
                            "other_count": 0,
                            "recent_error_runs": 2,
                        },
                        "beta": {
                            "source_id": "beta",
                            "pending_count": 1,
                            "blocked_count": 0,
                            "transient_count": 0,
                            "no_subtitles_count": 1,
                            "missing_artifact_count": 0,
                            "other_count": 0,
                            "recent_error_runs": 1,
                        },
                    },
                },
                snapshot_at=dt.datetime(2026, 3, 10, 1, 0, tzinfo=dt.timezone.utc),
            )
            self.mod.upsert_workspace_download_daily_metrics(
                connection=connection,
                source_ids=["alpha", "beta"],
                download_monitor={
                    "per_source": {
                        "alpha": {
                            "source_id": "alpha",
                            "pending_count": 2,
                            "blocked_count": 1,
                            "transient_count": 1,
                            "no_subtitles_count": 0,
                            "missing_artifact_count": 0,
                            "other_count": 0,
                            "recent_error_runs": 3,
                        },
                        "beta": {
                            "source_id": "beta",
                            "pending_count": 0,
                            "blocked_count": 0,
                            "transient_count": 0,
                            "no_subtitles_count": 0,
                            "missing_artifact_count": 0,
                            "other_count": 0,
                            "recent_error_runs": 0,
                        },
                    },
                },
                snapshot_at=dt.datetime(2026, 3, 12, 1, 0, tzinfo=dt.timezone.utc),
            )
            connection.commit()

            trend = self.mod.collect_workspace_download_error_trend(
                connection=connection,
                source_ids=[],
                window_days=7,
                end_date=dt.date(2026, 3, 12),
            )
        finally:
            connection.close()

        self.assertEqual(trend["window_days"], 7)
        self.assertEqual(trend["snapshot_days"], 2)
        points_by_date = {point["date"]: point for point in trend["points"]}
        self.assertEqual(points_by_date["2026-03-10"]["pending_count"], 4)
        self.assertEqual(points_by_date["2026-03-11"]["pending_count"], 4)
        self.assertEqual(points_by_date["2026-03-11"]["delta_pending_count"], 0)
        self.assertEqual(points_by_date["2026-03-12"]["pending_count"], 2)
        self.assertEqual(points_by_date["2026-03-12"]["transient_count"], 1)
        self.assertEqual(points_by_date["2026-03-12"]["recent_error_runs"], 3)
        self.assertEqual(trend["net_change"]["pending_count"], -2)

    def test_run_ytdlp_update_records_history_and_status(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"

[[sources]]
id = "alpha"
platform = "tiktok"
url = "https://example.com/@alpha"
enabled = true
data_dir = "alpha"
            """.strip()
            + "\n",
            encoding="utf-8",
        )

        with (
            mock.patch.object(
                self.mod,
                "resolve_effective_ytdlp_bin_from_config",
                return_value="/tmp/fake-ytdlp",
            ),
            mock.patch.object(
                self.mod,
                "probe_ytdlp_version",
                side_effect=["2026.3.1", "2026.3.3"],
            ),
            mock.patch.object(
                self.mod,
                "build_ytdlp_update_command",
                return_value=(["uv", "tool", "install", "yt-dlp", "--force"], None),
            ),
            mock.patch.object(
                self.mod,
                "run_command_with_output",
                return_value=(0, "uv tool install yt-dlp --force"),
            ),
        ):
            rc = self.mod.run_ytdlp_update(
                db_path=self.db_path,
                config_path=config_path,
                mode="uv",
                trigger="manual",
                uv_with_curl_cffi=True,
            )

        self.assertEqual(rc, 0)

        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(
                """
                SELECT status, manager, version_before, version_after, message
                FROM ytdlp_update_runs
                ORDER BY run_id DESC
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row["status"]), "updated")
            self.assertEqual(str(row["manager"]), "uv")
            self.assertEqual(str(row["version_before"]), "2026.3.1")
            self.assertEqual(str(row["version_after"]), "2026.3.3")
            self.assertIn("updated via uv", str(row["message"]))

            with mock.patch.object(
                self.mod,
                "resolve_effective_ytdlp_bin_from_config",
                return_value="/tmp/fake-ytdlp",
            ):
                status = self.mod.collect_workspace_ytdlp_status(
                    connection=connection,
                    config_path=config_path,
                    run_limit=4,
                )
        finally:
            connection.close()

        self.assertEqual(status["current_version"], "2026.3.3")
        self.assertIsNotNone(status["latest_updated"])
        self.assertEqual(status["latest_updated"]["status"], "updated")

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

[[auto_tags]]
tag = "Claude字幕50%+"
metric = "claude_subtitles_ready_playable_ratio"
gte = 0.5

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
                            "tags": ["English", "Funny"],
                        },
                        {
                            "id": "storiesofcz_likes",
                            "watch_kind": "likes",
                            "target_handle": "storiesofcz",
                            "enabled": True,
                            "tags": "Cooking, Science",
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
        self.assertEqual(override_source.tags, ["English", "Funny"])

        likes_source = by_id["storiesofcz_likes"]
        self.assertTrue(likes_source.enabled)
        self.assertEqual(likes_source.watch_kind, "likes")
        self.assertEqual(likes_source.target_handle, "storiesofcz")
        self.assertEqual(likes_source.url, "https://www.tiktok.com/@storiesofcz/liked")
        self.assertEqual(likes_source.origin, "managed")
        self.assertEqual(likes_source.tags, ["Cooking", "Science"])

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

[[auto_tags]]
tag = "Claude字幕50%+"
metric = "claude_subtitles_ready_playable_ratio"
gte = 0.5

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
            "tags": ["Funny", "English"],
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

        now_iso = dt.datetime(2026, 3, 9, 1, 2, tzinfo=dt.timezone.utc).isoformat()
        connection = sqlite3.connect(str(self.db_path))
        try:
            connection.execute(
                """
                INSERT INTO videos(
                    source_id,
                    video_id,
                    meta_path,
                    has_media,
                    audio_loudness_analyzed_at,
                    synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("storiesofcz_likes", "7611111111111111111", "/tmp/likes.info.json", 1, now_iso, now_iso),
            )
            connection.execute(
                """
                INSERT INTO subtitles(source_id, video_id, language, subtitle_path, ext)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("storiesofcz_likes", "7611111111111111111", "ja", "/tmp/likes.ja.vtt", "vtt"),
            )
            connection.commit()
        finally:
            connection.close()

        with urllib.request.urlopen(list_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            list_payload = json.loads(response.read().decode("utf-8"))
        by_id = {row["id"]: row for row in list_payload.get("targets", [])}
        self.assertIn("storiesofcz_likes", by_id)
        self.assertEqual(by_id["storiesofcz_likes"]["watch_kind"], "likes")
        self.assertEqual(by_id["storiesofcz_likes"]["url"], "https://www.tiktok.com/@storiesofcz/liked")
        self.assertEqual(by_id["storiesofcz_likes"]["manual_tags"], ["Funny", "English"])
        self.assertEqual(by_id["storiesofcz_likes"]["auto_tags"], ["Claude字幕50%+"])
        self.assertEqual(by_id["storiesofcz_likes"]["tags"], ["Funny", "English", "Claude字幕50%+"])

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

    def test_source_target_api_concurrent_upserts_preserve_all_updates(self):
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
        write_barrier = threading.Barrier(2)
        start_barrier = threading.Barrier(2)
        original_write = self.mod.write_managed_targets_payload
        errors: list[BaseException] = []

        def delayed_write(managed_path, payload):
            try:
                write_barrier.wait(timeout=0.5)
            except threading.BrokenBarrierError:
                pass
            self.mod.time.sleep(0.05)
            return original_write(managed_path, payload)

        def submit_upsert(source_id: str) -> None:
            payload = {
                "id": source_id,
                "watch_kind": "likes",
                "target_handle": source_id,
                "enabled": True,
            }
            request = urllib.request.Request(
                upsert_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                start_barrier.wait(timeout=2)
                with urllib.request.urlopen(request, timeout=5) as response:
                    self.assertEqual(response.status, 200)
            except BaseException as exc:  # pragma: no cover - thread coordination
                errors.append(exc)

        with mock.patch.object(self.mod, "write_managed_targets_payload", side_effect=delayed_write):
            threads = [
                threading.Thread(target=submit_upsert, args=("alpha_like",), daemon=True),
                threading.Thread(target=submit_upsert, args=("beta_like",), daemon=True),
            ]
            for item in threads:
                item.start()
            for item in threads:
                item.join(timeout=6)

        self.assertEqual(errors, [])
        with urllib.request.urlopen(list_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            payload = json.loads(response.read().decode("utf-8"))
        by_id = {row["id"]: row for row in payload.get("targets", [])}
        self.assertIn("alpha_like", by_id)
        self.assertIn("beta_like", by_id)

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

    def test_load_config_parses_auto_tag_rules(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"

[[auto_tags]]
tag = "Claude字幕50%+"
metric = "claude_subtitles_ready_playable_ratio"
gte = 0.5
min_total_videos = 10

[[sources]]
id = "alpha"
platform = "tiktok"
url = "https://www.tiktok.com/@alpha"
enabled = true
            """.strip()
            + "\n",
            encoding="utf-8",
        )

        global_config, sources = self.mod.load_config(config_path)

        self.assertEqual(len(sources), 1)
        self.assertEqual(len(global_config.auto_tag_rules), 1)
        rule = global_config.auto_tag_rules[0]
        self.assertEqual(rule.tag, "Claude字幕50%+")
        self.assertEqual(rule.metric, "claude_subtitles_ready_playable_ratio")
        self.assertEqual(rule.comparator, "gte")
        self.assertEqual(rule.threshold, 0.5)
        self.assertEqual(rule.min_total_videos, 10)

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

        with (
            mock.patch.object(self.mod.random, "uniform", return_value=1.5),
            mock.patch.object(self.mod.random, "random", return_value=1.0),
            mock.patch.object(self.mod.subprocess, "run", side_effect=fake_run),
        ):
            ids = self.mod.discover_playlist_window_ids(
                source=source,
                playlist_start=1,
                playlist_end=5,
                dry_run=False,
            )

        self.assertEqual(ids, ["7611111111111111111", "7612222222222222222"])
        command = captured["command"]
        self.assertIn("--sleep-requests", command)
        self.assertIn("1.5", command)
        self.assertIn("--sleep-interval", command)
        self.assertIn("--max-sleep-interval", command)
        self.assertIn("--retry-sleep", command)
        self.assertNotIn("--ignore-errors", command)

    def test_compute_effective_sleep_requests_seconds_respects_minimum_floor(self):
        source = self.mod.SourceConfig(
            id="storiesofcz",
            platform="tiktok",
            url="https://www.tiktok.com/@storiesofcz",
            tags=[],
            watch_kind="posts",
            target_handle=None,
            enabled=True,
            data_dir=self.workspace_root,
            media_dir=self.workspace_root / "media",
            subs_dir=self.workspace_root / "subs",
            meta_dir=self.workspace_root / "meta",
            media_archive=self.workspace_root / "archives" / "media.txt",
            subs_archive=self.workspace_root / "archives" / "subs.txt",
            urls_file=self.workspace_root / "archives" / "urls.txt",
            handle="storiesofcz",
            video_url_template=None,
            video_id_regex=r"_(\d{10,})_",
            ytdlp_bin="yt-dlp",
            ytdlp_impersonate=None,
            cookies_browser=None,
            cookies_file=None,
            video_format="bv*+ba/best",
            sub_langs="en",
            sub_format="vtt",
            sleep_interval=2,
            max_sleep_interval=6,
            retry_sleep=5,
            sleep_requests=3.0,
            media_discovery_interval_hours=24.0,
            playlist_end=200,
            break_on_existing=False,
            break_per_input=False,
            lazy_playlist=False,
            backfill_enabled=False,
            backfill_start=None,
            backfill_window=200,
            backfill_windows_per_run=1,
            asr_enabled=False,
            asr_dir=self.workspace_root / "asr",
            asr_command=[],
            asr_max_per_run=20,
            asr_timeout_sec=0,
            asr_prefer_exts=["srt", "vtt"],
            media_output_template="%(id)s.%(ext)s",
            subs_output_template="%(id)s.%(language)s.%(ext)s",
            meta_output_template="%(id)s.%(ext)s",
            origin="config",
        )

        with (
            mock.patch.object(self.mod.random, "uniform", side_effect=lambda lower, upper: lower),
            mock.patch.object(self.mod.random, "random", return_value=1.0),
        ):
            effective = self.mod.compute_effective_sleep_requests_seconds(source)

        self.assertEqual(effective, 5.0)
        self.assertEqual(self.mod.format_ytdlp_sleep_seconds(effective), "5.0")

    def test_compute_effective_sleep_requests_seconds_can_add_long_pause(self):
        source = self.mod.SourceConfig(
            id="storiesofcz",
            platform="tiktok",
            url="https://www.tiktok.com/@storiesofcz",
            tags=[],
            watch_kind="posts",
            target_handle=None,
            enabled=True,
            data_dir=self.workspace_root,
            media_dir=self.workspace_root / "media",
            subs_dir=self.workspace_root / "subs",
            meta_dir=self.workspace_root / "meta",
            media_archive=self.workspace_root / "archives" / "media.txt",
            subs_archive=self.workspace_root / "archives" / "subs.txt",
            urls_file=self.workspace_root / "archives" / "urls.txt",
            handle="storiesofcz",
            video_url_template=None,
            video_id_regex=r"_(\d{10,})_",
            ytdlp_bin="yt-dlp",
            ytdlp_impersonate=None,
            cookies_browser=None,
            cookies_file=None,
            video_format="bv*+ba/best",
            sub_langs="en",
            sub_format="vtt",
            sleep_interval=2,
            max_sleep_interval=6,
            retry_sleep=5,
            sleep_requests=8.0,
            media_discovery_interval_hours=24.0,
            playlist_end=200,
            break_on_existing=False,
            break_per_input=False,
            lazy_playlist=False,
            backfill_enabled=False,
            backfill_start=None,
            backfill_window=200,
            backfill_windows_per_run=1,
            asr_enabled=False,
            asr_dir=self.workspace_root / "asr",
            asr_command=[],
            asr_max_per_run=20,
            asr_timeout_sec=0,
            asr_prefer_exts=["srt", "vtt"],
            media_output_template="%(id)s.%(ext)s",
            subs_output_template="%(id)s.%(language)s.%(ext)s",
            meta_output_template="%(id)s.%(ext)s",
            origin="config",
        )

        with (
            mock.patch.object(self.mod.random, "uniform", side_effect=[5.2, 1.8]),
            mock.patch.object(self.mod.random, "random", return_value=0.0),
        ):
            effective = self.mod.compute_effective_sleep_requests_seconds(source)

        self.assertEqual(effective, 7.0)
        self.assertGreaterEqual(effective, 5.0)

    def test_workspace_api_source_processing_includes_source_tags(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"

[[auto_tags]]
tag = "Claude字幕50%+"
metric = "claude_subtitles_ready_playable_ratio"
gte = 0.5

[[sources]]
id = "alpha"
platform = "tiktok"
url = "https://www.tiktok.com/@alpha"
enabled = true
tags = ["English", "Funny"]
            """.strip()
            + "\n",
            encoding="utf-8",
        )

        now_iso = dt.datetime(2026, 3, 9, 1, 2, tzinfo=dt.timezone.utc).isoformat()
        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(
                """
                INSERT INTO videos(
                    source_id,
                    video_id,
                    meta_path,
                    has_media,
                    audio_loudness_analyzed_at,
                    synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alpha", "7611111111111111111", "/tmp/alpha.info.json", 1, now_iso, now_iso),
            )
            connection.execute(
                """
                INSERT INTO subtitles(source_id, video_id, language, subtitle_path, ext)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("alpha", "7611111111111111111", "en", "/tmp/alpha.en.vtt", "vtt"),
            )
            connection.execute(
                """
                INSERT INTO subtitles(source_id, video_id, language, subtitle_path, ext)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("alpha", "7611111111111111111", "ja", "/tmp/alpha.ja.vtt", "vtt"),
            )
            connection.commit()
        finally:
            connection.close()

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
        workspace_url = f"http://{host}:{port}/api/workspace"
        with urllib.request.urlopen(workspace_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            payload = json.loads(response.read().decode("utf-8"))

        sources = payload.get("source_processing", {}).get("sources", [])
        self.assertEqual(len(sources), 1)
        self.assertEqual(sources[0]["source_id"], "alpha")
        self.assertEqual(sources[0]["manual_tags"], ["English", "Funny"])
        self.assertEqual(sources[0]["auto_tags"], ["Claude字幕50%+"])
        self.assertEqual(sources[0]["tags"], ["English", "Funny", "Claude字幕50%+"])
        trend = payload.get("source_processing", {}).get("daily_trend", {})
        points = trend.get("points", [])
        self.assertEqual(trend.get("window_days"), 30)
        self.assertTrue(points)
        self.assertEqual(points[-1]["media_ready"], 1)
        self.assertEqual(points[-1]["ja_subtitles_ready_playable"], 1)

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

    def test_is_blocked_or_forbidden_error_requires_tiktok_context(self):
        self.assertFalse(
            self.mod.is_blocked_or_forbidden_error("HTTP Error 403: Forbidden")
        )
        self.assertTrue(
            self.mod.is_blocked_or_forbidden_error(
                "ERROR: [TikTok] HTTP Error 403: Forbidden"
            )
        )
        self.assertTrue(
            self.mod.is_blocked_or_forbidden_error(
                "ERROR: [TikTok] status 10204"
            )
        )

    def test_schedule_next_retry_iso_uses_structural_backoff_for_missing_artifact_error(self):
        before = dt.datetime.now(dt.timezone.utc)
        retry_at = dt.datetime.fromisoformat(
            self.mod.schedule_next_retry_iso(
                1,
                error_message="subtitle file missing after download attempt",
            )
        )
        after = dt.datetime.now(dt.timezone.utc)
        delay_min = (retry_at - before).total_seconds()
        delay_max = (retry_at - after).total_seconds()
        self.assertGreaterEqual(delay_min, (30 * 60) - 5)
        self.assertLessEqual(delay_max, (30 * 60) + 5)

    def test_schedule_next_retry_iso_uses_long_backoff_for_requested_subtitles_unavailable(self):
        before = dt.datetime.now(dt.timezone.utc)
        retry_at = dt.datetime.fromisoformat(
            self.mod.schedule_next_retry_iso(
                1,
                error_message="There are no subtitles for the requested languages",
            )
        )
        after = dt.datetime.now(dt.timezone.utc)
        delay_min = (retry_at - before).total_seconds()
        delay_max = (retry_at - after).total_seconds()
        self.assertGreaterEqual(delay_min, (14 * 86400) - 5)
        self.assertLessEqual(delay_max, (14 * 86400) + 5)

    def test_schedule_next_retry_iso_uses_transient_tiktok_webpage_backoff(self):
        before = dt.datetime.now(dt.timezone.utc)
        retry_at = dt.datetime.fromisoformat(
            self.mod.schedule_next_retry_iso(
                1,
                error_message=(
                    "ERROR: [TikTok] 7611111111111111111: "
                    "Unable to extract universal data for rehydration"
                ),
            )
        )
        after = dt.datetime.now(dt.timezone.utc)
        delay_min = (retry_at - before).total_seconds()
        delay_max = (retry_at - after).total_seconds()
        self.assertTrue(
            self.mod.is_tiktok_transient_webpage_error(
                "ERROR: [TikTok] 7611111111111111111: Unable to extract universal data for rehydration"
            )
        )
        self.assertGreaterEqual(delay_min, (30 * 60) - 5)
        self.assertLessEqual(delay_max, (30 * 60) + 5)

    def test_source_network_cooldown_error_only_matches_blocked_errors(self):
        self.assertTrue(
            self.mod.is_source_network_cooldown_error(
                "ERROR: [TikTok] Your IP address is blocked from accessing this post"
            )
        )
        self.assertFalse(
            self.mod.is_source_network_cooldown_error(
                "ERROR: [TikTok] 7611111111111111111: Unable to extract universal data for rehydration"
            )
        )

    def test_get_source_network_cooldown_state_clears_transient_webpage_cooldown(self):
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            future_block = "2026-03-12T00:00:00+00:00"
            self.mod.upsert_source_access_state(
                connection=connection,
                source_id="storiesofcz",
                blocked_until=future_block,
                last_error=(
                    "ERROR: [TikTok] 7611111111111111111: "
                    "Unable to extract universal data for rehydration"
                ),
                updated_at="2026-03-11T00:00:00+00:00",
            )
            connection.commit()

            active, remaining_hours, blocked_until, last_error = self.mod.get_source_network_cooldown_state(
                connection=connection,
                source_id="storiesofcz",
                now_dt=dt.datetime(2026, 3, 11, 1, 0, tzinfo=dt.timezone.utc),
            )

            row = connection.execute(
                "SELECT blocked_until FROM source_access_state WHERE source_id = ?",
                ("storiesofcz",),
            ).fetchone()
        finally:
            connection.close()

        self.assertFalse(active)
        self.assertEqual(remaining_hours, 0.0)
        self.assertIsNone(blocked_until)
        self.assertIn("rehydration", str(last_error).lower())
        self.assertIsNotNone(row)
        self.assertIsNone(row[0])

    def test_show_queue_status_report_summarizes_due_wait_dead(self):
        source_root = self.workspace_root / "storiesofcz_queue_status"
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

        now_dt = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
        now_iso = now_dt.isoformat()
        past_iso = (now_dt - dt.timedelta(days=1)).isoformat()
        future_iso = (now_dt + dt.timedelta(days=1)).isoformat()
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.upsert_source(connection, source, now_iso)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="subs",
                video_id="7624444444444444441",
                now_iso=now_iso,
                priority=1,
            )
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="meta",
                video_id="7624444444444444442",
                now_iso=now_iso,
                priority=1,
            )
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="media",
                video_id="7624444444444444443",
                now_iso=now_iso,
                priority=1,
            )
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="asr",
                video_id="7624444444444444444",
                now_iso=now_iso,
                priority=1,
            )
            connection.execute(
                """
                UPDATE work_items
                SET status = 'error',
                    attempt_count = 2,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE source_id = ? AND stage = 'subs' AND video_id = ?
                """,
                (
                    past_iso,
                    "subtitle file missing after download attempt",
                    now_iso,
                    source.id,
                    "7624444444444444441",
                ),
            )
            connection.execute(
                """
                UPDATE work_items
                SET status = 'error',
                    attempt_count = 1,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE source_id = ? AND stage = 'meta' AND video_id = ?
                """,
                (
                    future_iso,
                    "temporary metadata download error",
                    now_iso,
                    source.id,
                    "7624444444444444442",
                ),
            )
            connection.execute(
                """
                UPDATE work_items
                SET status = 'dead',
                    attempt_count = 5,
                    last_error = ?,
                    updated_at = ?
                WHERE source_id = ? AND stage = 'media' AND video_id = ?
                """,
                (
                    "HTTP Error 403: Forbidden",
                    now_iso,
                    source.id,
                    "7624444444444444443",
                ),
            )
            connection.execute(
                """
                UPDATE work_items
                SET status = 'success',
                    attempt_count = 2,
                    updated_at = ?,
                    finished_at = ?
                WHERE source_id = ? AND stage = 'asr' AND video_id = ?
                """,
                (
                    now_iso,
                    now_iso,
                    source.id,
                    "7624444444444444444",
                ),
            )
            self.mod.upsert_source_access_state(
                connection=connection,
                source_id=source.id,
                blocked_until=future_iso,
                last_blocked_at=now_iso,
                last_error="ERROR: [TikTok] Your IP address is blocked from accessing this post",
                updated_at=now_iso,
            )
            connection.commit()
        finally:
            connection.close()

        output = io.StringIO()
        with redirect_stdout(output):
            self.mod.show_queue_status_report(
                sources=[source],
                db_path=self.db_path,
                limit=10,
            )

        rendered = output.getvalue()
        self.assertIn("queue-status: storiesofcz", rendered)
        self.assertIn("source network cooldown:", rendered)
        self.assertIn(f"until={future_iso}", rendered)
        self.assertIn("queue unresolved total=3", rendered)
        self.assertIn("retry_due=1", rendered)
        self.assertIn("retry_wait=1", rendered)
        self.assertIn("dead=1", rendered)
        self.assertIn("subtitle file missing after download attempt", rendered)
        self.assertIn("HTTP Error 403: Forbidden", rendered)
        self.assertIn("recovered by retry total=1", rendered)
        self.assertIn("recent recovered:", rendered)
        self.assertIn("stage=asr", rendered)
        self.assertIn("retries=1", rendered)

    def test_show_queue_status_report_only_unresolved_filters_sources(self):
        source_root_a = self.workspace_root / "storiesofcz_queue_status_only_unresolved_a"
        source_root_b = self.workspace_root / "factswithcori_queue_status_only_unresolved_b"
        source_root_a.mkdir(parents=True, exist_ok=True)
        source_root_b.mkdir(parents=True, exist_ok=True)
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
data_dir = "{source_root_a}"

[[sources]]
id = "factswithcori"
platform = "tiktok"
url = "https://www.tiktok.com/@factswithcori"
enabled = true
data_dir = "{source_root_b}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        by_id = {source.id: source for source in sources}
        source_a = by_id["storiesofcz"]
        source_b = by_id["factswithcori"]

        now_iso = "2026-03-07T00:00:00+00:00"
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.upsert_source(connection, source_a, now_iso)
            self.mod.upsert_source(connection, source_b, now_iso)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source_a.id,
                stage="translate",
                video_id="7624777777777777771",
                now_iso=now_iso,
                priority=1,
            )
            connection.execute(
                """
                UPDATE work_items
                SET status = 'dead',
                    attempt_count = 8,
                    last_error = ?,
                    updated_at = ?
                WHERE source_id = ? AND stage = 'translate' AND video_id = ?
                """,
                (
                    "work item execution exception: tuple indices must be integers or slices, not str",
                    now_iso,
                    source_a.id,
                    "7624777777777777771",
                ),
            )
            connection.commit()
        finally:
            connection.close()

        output = io.StringIO()
        with redirect_stdout(output):
            self.mod.show_queue_status_report(
                sources=[source_a, source_b],
                db_path=self.db_path,
                limit=10,
                only_unresolved=True,
            )

        rendered = output.getvalue()
        self.assertIn("queue-status: storiesofcz", rendered)
        self.assertNotIn("queue-status: factswithcori", rendered)

    def test_requeue_work_items_filters_by_stage_status_and_error(self):
        source_root = self.workspace_root / "storiesofcz_queue_requeue"
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

        now_iso = "2026-03-07T00:00:00+00:00"
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.upsert_source(connection, source, now_iso)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="translate",
                video_id="7625555555555555551",
                now_iso=now_iso,
                priority=1,
            )
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="translate",
                video_id="7625555555555555552",
                now_iso=now_iso,
                priority=1,
            )
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="subs",
                video_id="7625555555555555553",
                now_iso=now_iso,
                priority=1,
            )
            connection.execute(
                """
                UPDATE work_items
                SET status = 'dead',
                    attempt_count = 8,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE source_id = ? AND stage = 'translate' AND video_id = ?
                """,
                (
                    "2026-03-08T00:00:00+00:00",
                    "work item execution exception: tuple indices must be integers or slices, not str",
                    now_iso,
                    source.id,
                    "7625555555555555551",
                ),
            )
            connection.execute(
                """
                UPDATE work_items
                SET status = 'dead',
                    attempt_count = 8,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE source_id = ? AND stage = 'translate' AND video_id = ?
                """,
                (
                    "2026-03-08T00:00:00+00:00",
                    "HTTP Error 403: Forbidden",
                    now_iso,
                    source.id,
                    "7625555555555555552",
                ),
            )
            connection.execute(
                """
                UPDATE work_items
                SET status = 'dead',
                    attempt_count = 8,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE source_id = ? AND stage = 'subs' AND video_id = ?
                """,
                (
                    "2026-03-08T00:00:00+00:00",
                    "work item execution exception: tuple indices must be integers or slices, not str",
                    now_iso,
                    source.id,
                    "7625555555555555553",
                ),
            )
            connection.commit()
        finally:
            connection.close()

        self.mod.requeue_work_items(
            sources=[source],
            db_path=self.db_path,
            stages=["translate"],
            statuses=["dead"],
            error_contains="tuple indices must be integers or slices, not str",
            limit=0,
            dry_run=False,
            reset_attempts=False,
        )

        connection_check = sqlite3.connect(str(self.db_path))
        try:
            row_target = connection_check.execute(
                """
                SELECT status, attempt_count, next_retry_at
                FROM work_items
                WHERE source_id = ? AND stage = 'translate' AND video_id = ?
                """,
                (source.id, "7625555555555555551"),
            ).fetchone()
            self.assertIsNotNone(row_target)
            self.assertEqual(str(row_target[0]), "queued")
            self.assertEqual(int(row_target[1]), 8)
            self.assertIsNone(row_target[2])

            row_other_translate = connection_check.execute(
                """
                SELECT status, next_retry_at
                FROM work_items
                WHERE source_id = ? AND stage = 'translate' AND video_id = ?
                """,
                (source.id, "7625555555555555552"),
            ).fetchone()
            self.assertIsNotNone(row_other_translate)
            self.assertEqual(str(row_other_translate[0]), "dead")
            self.assertEqual(str(row_other_translate[1]), "2026-03-08T00:00:00+00:00")

            row_other_stage = connection_check.execute(
                """
                SELECT status, next_retry_at
                FROM work_items
                WHERE source_id = ? AND stage = 'subs' AND video_id = ?
                """,
                (source.id, "7625555555555555553"),
            ).fetchone()
            self.assertIsNotNone(row_other_stage)
            self.assertEqual(str(row_other_stage[0]), "dead")
            self.assertEqual(str(row_other_stage[1]), "2026-03-08T00:00:00+00:00")
        finally:
            connection_check.close()

    def test_queue_recover_known_profile_requeues_expected_items(self):
        source_root = self.workspace_root / "storiesofcz_queue_recover_known"
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

        now_iso = "2026-03-07T00:00:00+00:00"
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            self.mod.upsert_source(connection, source, now_iso)
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="translate",
                video_id="7626666666666666661",
                now_iso=now_iso,
                priority=1,
            )
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="translate",
                video_id="7626666666666666662",
                now_iso=now_iso,
                priority=1,
            )
            self.mod.enqueue_work_item(
                connection=connection,
                source_id=source.id,
                stage="meta",
                video_id="7626666666666666663",
                now_iso=now_iso,
                priority=1,
            )
            connection.execute(
                """
                UPDATE work_items
                SET status = 'dead',
                    attempt_count = 8,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE source_id = ? AND stage = 'translate' AND video_id = ?
                """,
                (
                    "2026-03-08T00:00:00+00:00",
                    "work item execution exception: tuple indices must be integers or slices, not str",
                    now_iso,
                    source.id,
                    "7626666666666666661",
                ),
            )
            connection.execute(
                """
                UPDATE work_items
                SET status = 'dead',
                    attempt_count = 8,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE source_id = ? AND stage = 'translate' AND video_id = ?
                """,
                (
                    "2026-03-08T00:00:00+00:00",
                    "HTTP Error 403: Forbidden",
                    now_iso,
                    source.id,
                    "7626666666666666662",
                ),
            )
            connection.execute(
                """
                UPDATE work_items
                SET status = 'dead',
                    attempt_count = 8,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE source_id = ? AND stage = 'meta' AND video_id = ?
                """,
                (
                    "2026-03-08T00:00:00+00:00",
                    "work item execution exception: tuple indices must be integers or slices, not str",
                    now_iso,
                    source.id,
                    "7626666666666666663",
                ),
            )
            connection.commit()
        finally:
            connection.close()

        self.mod.run_queue_recover_known(
            sources=[source],
            db_path=self.db_path,
            profiles=["translate-row-factory"],
            limit=0,
            dry_run=False,
            reset_attempts=False,
        )

        connection_check = sqlite3.connect(str(self.db_path))
        try:
            matched_row = connection_check.execute(
                """
                SELECT status, next_retry_at
                FROM work_items
                WHERE source_id = ? AND stage = 'translate' AND video_id = ?
                """,
                (source.id, "7626666666666666661"),
            ).fetchone()
            self.assertIsNotNone(matched_row)
            self.assertEqual(str(matched_row[0]), "queued")
            self.assertIsNone(matched_row[1])

            non_match_same_stage = connection_check.execute(
                """
                SELECT status, next_retry_at
                FROM work_items
                WHERE source_id = ? AND stage = 'translate' AND video_id = ?
                """,
                (source.id, "7626666666666666662"),
            ).fetchone()
            self.assertIsNotNone(non_match_same_stage)
            self.assertEqual(str(non_match_same_stage[0]), "dead")
            self.assertEqual(
                str(non_match_same_stage[1]),
                "2026-03-08T00:00:00+00:00",
            )

            non_match_other_stage = connection_check.execute(
                """
                SELECT status, next_retry_at
                FROM work_items
                WHERE source_id = ? AND stage = 'meta' AND video_id = ?
                """,
                (source.id, "7626666666666666663"),
            ).fetchone()
            self.assertIsNotNone(non_match_other_stage)
            self.assertEqual(str(non_match_other_stage[0]), "dead")
            self.assertEqual(
                str(non_match_other_stage[1]),
                "2026-03-08T00:00:00+00:00",
            )
        finally:
            connection_check.close()

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
            self.assertIn("source_access_state", tables)
            self.assertIn("worker_heartbeats", tables)
        finally:
            connection.close()

    def test_record_source_network_access_success_clears_cooldown_and_sets_spacing(self):
        source_root = self.workspace_root / "storiesofcz_source_access_state"
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
            now_dt = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
            now_iso = now_dt.isoformat()
            blocked_until = (now_dt + dt.timedelta(hours=6)).isoformat()
            self.mod.upsert_source_access_state(
                connection=connection,
                source_id=source.id,
                blocked_until=blocked_until,
                last_blocked_at=now_iso,
                last_error="ERROR: [TikTok] Your IP address is blocked from accessing this post",
                updated_at=now_iso,
            )

            next_request = self.mod.record_source_network_access(
                connection=connection,
                source=source,
                request_at=now_iso,
                succeeded=True,
                clear_cooldown=True,
                last_error=None,
            )

            row = connection.execute(
                """
                SELECT blocked_until, last_request_at, next_request_not_before, last_success_at, last_error
                FROM source_access_state
                WHERE source_id = ?
                """,
                (source.id,),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertIsNone(row[0])
            self.assertEqual(str(row[1]), now_iso)
            self.assertEqual(row[2], next_request)
            self.assertEqual(str(row[3]), now_iso)
            self.assertIsNone(row[4])

            expected_delay = self.mod.compute_source_network_min_interval_seconds(source)
            if expected_delay <= 0:
                self.assertIsNone(next_request)
            else:
                self.assertIsNotNone(next_request)
                next_request_dt = dt.datetime.fromisoformat(str(next_request))
                delay_seconds = (next_request_dt - now_dt).total_seconds()
                self.assertGreaterEqual(delay_seconds, expected_delay - 1)
                self.assertLessEqual(delay_seconds, expected_delay + 1)
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

    def test_enqueue_source_media_discovery_skips_active_network_cooldown(self):
        source_root = self.workspace_root / "storiesofcz_queue_blocked"
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
            now_dt = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
            now_iso = now_dt.isoformat()
            blocked_until = (now_dt + dt.timedelta(hours=6)).isoformat()
            self.mod.upsert_source_access_state(
                connection=connection,
                source_id=source.id,
                blocked_until=blocked_until,
                last_blocked_at=now_iso,
                last_error="ERROR: [TikTok] Your IP address is blocked from accessing this post",
                updated_at=now_iso,
            )
            connection.commit()

            with mock.patch.object(
                self.mod,
                "discover_playlist_window_ids",
                return_value=["7611111111111111111"],
            ) as discover_mock:
                discovered, inserted, requeued = self.mod.enqueue_source_media_discovery(
                    connection=connection,
                    source=source,
                    dry_run=False,
                    run_label="sync-queue",
                    enforce_poll_interval=True,
                )
            self.assertEqual((discovered, inserted, requeued), (0, 0, 0))
            self.assertEqual(discover_mock.call_count, 0)
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

    def test_enqueue_media_work_items_interleaves_sources_by_priority(self):
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            now_iso = "2026-03-07T00:00:00+00:00"
            self.mod.enqueue_media_work_items(
                connection=connection,
                source_id="source-a",
                video_ids=["7611000000000000001", "7611000000000000002", "7611000000000000003"],
                now_iso=now_iso,
                source_slot=0,
                source_stride=2,
            )
            self.mod.enqueue_media_work_items(
                connection=connection,
                source_id="source-b",
                video_ids=["7622000000000000001", "7622000000000000002", "7622000000000000003"],
                now_iso=now_iso,
                source_slot=1,
                source_stride=2,
            )
            connection.commit()

            rows = connection.execute(
                """
                SELECT source_id, video_id, priority
                FROM work_items
                WHERE stage = 'media'
                ORDER BY priority ASC, id ASC
                """
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    ("source-a", "7611000000000000001", 0),
                    ("source-b", "7622000000000000001", 1),
                    ("source-a", "7611000000000000002", 2),
                    ("source-b", "7622000000000000002", 3),
                    ("source-a", "7611000000000000003", 4),
                    ("source-b", "7622000000000000003", 5),
                ],
            )
        finally:
            connection.close()

    def test_lease_next_work_item_avoids_same_source_when_possible(self):
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            now_dt = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
            now_iso = now_dt.isoformat()
            self.mod.enqueue_work_item(
                connection=connection,
                source_id="source-a",
                stage="meta",
                video_id="7613000000000000001",
                now_iso=now_iso,
                priority=0,
            )
            self.mod.enqueue_work_item(
                connection=connection,
                source_id="source-a",
                stage="meta",
                video_id="7613000000000000002",
                now_iso=now_iso,
                priority=1,
            )
            self.mod.enqueue_work_item(
                connection=connection,
                source_id="source-b",
                stage="meta",
                video_id="7624000000000000001",
                now_iso=now_iso,
                priority=10,
            )
            connection.commit()

            leased_1 = self.mod.lease_next_work_item(
                connection=connection,
                worker_id="worker-a",
                stages=["meta"],
                lease_seconds=600,
            )
            self.assertIsNotNone(leased_1)
            self.assertEqual(str(leased_1["source_id"]), "source-a")

            leased_2 = self.mod.lease_next_work_item(
                connection=connection,
                worker_id="worker-b",
                stages=["meta"],
                lease_seconds=600,
                avoid_source_id="source-a",
            )
            self.assertIsNotNone(leased_2)
            self.assertEqual(str(leased_2["source_id"]), "source-b")

            leased_3 = self.mod.lease_next_work_item(
                connection=connection,
                worker_id="worker-c",
                stages=["meta"],
                lease_seconds=600,
                avoid_source_id="source-b",
            )
            self.assertIsNotNone(leased_3)
            self.assertEqual(str(leased_3["source_id"]), "source-a")
        finally:
            connection.close()

    def test_lease_next_work_item_skips_network_stage_for_blocked_source(self):
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            now_dt = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
            now_iso = now_dt.isoformat()
            self.mod.enqueue_work_item(
                connection=connection,
                source_id="source-a",
                stage="media",
                video_id="7615000000000000001",
                now_iso=now_iso,
                priority=0,
            )
            self.mod.enqueue_work_item(
                connection=connection,
                source_id="source-b",
                stage="media",
                video_id="7625000000000000001",
                now_iso=now_iso,
                priority=10,
            )
            self.mod.upsert_source_access_state(
                connection=connection,
                source_id="source-a",
                blocked_until=(now_dt + dt.timedelta(hours=6)).isoformat(),
                last_blocked_at=now_iso,
                last_error="ERROR: [TikTok] Your IP address is blocked from accessing this post",
                updated_at=now_iso,
            )
            connection.commit()

            leased = self.mod.lease_next_work_item(
                connection=connection,
                worker_id="worker-media",
                stages=["media"],
                lease_seconds=600,
            )
            self.assertIsNotNone(leased)
            self.assertEqual(str(leased["source_id"]), "source-b")
        finally:
            connection.close()

    def test_lease_next_work_item_skips_network_stage_for_paced_source(self):
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            now_dt = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
            now_iso = now_dt.isoformat()
            self.mod.enqueue_work_item(
                connection=connection,
                source_id="source-a",
                stage="meta",
                video_id="7615100000000000001",
                now_iso=now_iso,
                priority=0,
            )
            self.mod.enqueue_work_item(
                connection=connection,
                source_id="source-b",
                stage="meta",
                video_id="7625100000000000001",
                now_iso=now_iso,
                priority=10,
            )
            self.mod.upsert_source_access_state(
                connection=connection,
                source_id="source-a",
                next_request_not_before=(now_dt + dt.timedelta(minutes=5)).isoformat(),
                updated_at=now_iso,
            )
            connection.commit()

            leased = self.mod.lease_next_work_item(
                connection=connection,
                worker_id="worker-meta",
                stages=["meta"],
                lease_seconds=600,
            )
            self.assertIsNotNone(leased)
            self.assertEqual(str(leased["source_id"]), "source-b")
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

        def fake_translate_local_for_video(*args, **kwargs):
            connection_arg = kwargs.get("connection")
            self.assertIsNotNone(connection_arg)
            self.assertIs(connection_arg.row_factory, sqlite3.Row)
            return (True, None)

        with mock.patch.object(
            self.mod,
            "run_translate_local_for_video",
            side_effect=fake_translate_local_for_video,
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
                with mock.patch.object(self.mod, "run_command_with_output", return_value=(0, "")):
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

    def test_sync_source_subtitles_are_chunked_into_multiple_ytdlp_runs(self):
        source_root = self.workspace_root / "storiesofcz_subs_chunked"
        source_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"
sleep_requests = 1.25

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
            candidate_ids = [
                "7617777777777777701",
                "7617777777777777702",
                "7617777777777777703",
                "7617777777777777704",
                "7617777777777777705",
                "7617777777777777706",
            ]

            with (
                mock.patch.object(self.mod.random, "uniform", side_effect=[0.9, 1.1, 2.2]),
                mock.patch.object(self.mod.random, "random", return_value=1.0),
                mock.patch.object(
                    self.mod,
                    "run_command_with_output",
                    return_value=(0, ""),
                ) as run_command_mock,
            ):
                self.mod.sync_source(
                    source=source,
                    dry_run=False,
                    skip_media=True,
                    skip_subs=False,
                    skip_meta=True,
                    connection=connection,
                    metadata_candidate_ids=candidate_ids,
                    strict_candidate_scope=True,
                )

            self.assertEqual(run_command_mock.call_count, 2)
            first_command = list(run_command_mock.call_args_list[0].args[0])
            second_command = list(run_command_mock.call_args_list[1].args[0])
            self.assertIn("--sleep-requests", first_command)
            self.assertIn("--sleep-requests", second_command)
            self.assertIn("--sub-langs", first_command)
            merged_sub_langs = first_command[first_command.index("--sub-langs") + 1]
            self.assertEqual(merged_sub_langs, "en.*,en,und,ja.*,ja,jp.*,jpn.*")
            first_sleep = first_command[first_command.index("--sleep-requests") + 1]
            second_sleep = second_command[second_command.index("--sleep-requests") + 1]
            self.assertEqual(first_sleep, "1.1")
            self.assertEqual(second_sleep, "2.2")
        finally:
            connection.close()

    def test_run_legacy_sync_sources_round_robins_subtitle_chunks_by_source(self):
        alpha_root = self.workspace_root / "alpha_subs_rr"
        beta_root = self.workspace_root / "beta_subs_rr"
        alpha_root.mkdir(parents=True, exist_ok=True)
        beta_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "alpha"
platform = "tiktok"
url = "https://www.tiktok.com/@alpha"
enabled = true
data_dir = "{alpha_root}"

[[sources]]
id = "beta"
platform = "tiktok"
url = "https://www.tiktok.com/@beta"
enabled = true
data_dir = "{beta_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        by_id = {source.id: source for source in sources}
        alpha = by_id["alpha"]
        beta = by_id["beta"]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            now_iso = "2026-03-09T00:00:00+00:00"
            for source in (alpha, beta):
                for index in range(1, 7):
                    video_id = f"76177777777777777{index:02d}{0 if source.id == 'alpha' else 1}"
                    connection.execute(
                        """
                        INSERT INTO videos(source_id, video_id, media_path, has_media, has_subtitles, synced_at)
                        VALUES (?, ?, ?, 1, 0, ?)
                        """,
                        (source.id, video_id, str(source.media_dir / f"{video_id}.mp4"), now_iso),
                    )
            connection.commit()

            def fake_run_command(command, dry_run=False):
                self.assertFalse(dry_run)
                args = list(command)
                template = str(args[args.index("-o") + 1])
                urls_path = Path(args[args.index("-a") + 1])
                for url in urls_path.read_text(encoding="utf-8").splitlines():
                    video_id = url.rstrip("/").rsplit("/", 1)[-1]
                    subtitle_path = Path(
                        template
                        .replace("%(id)s", video_id)
                        .replace("%(language)s", "eng-US")
                        .replace("%(ext)s", "vtt")
                    )
                    subtitle_path.parent.mkdir(parents=True, exist_ok=True)
                    subtitle_path.write_text(
                        "WEBVTT\n\n00:00.000 --> 00:01.000\nhello\n",
                        encoding="utf-8",
                    )
                return (0, "")

            stdout_capture = io.StringIO()
            with (
                mock.patch.object(self.mod, "run_command_with_output", side_effect=fake_run_command),
                redirect_stdout(stdout_capture),
            ):
                self.mod.run_legacy_sync_sources(
                    sources=[alpha, beta],
                    dry_run=False,
                    skip_media=True,
                    skip_subs=False,
                    skip_meta=True,
                    connection=connection,
                    metered_media_mode="off",
                    metered_min_archive_ids=0,
                    metered_playlist_end=5,
                )
        finally:
            connection.close()

        output = stdout_capture.getvalue()
        alpha_chunk_1 = output.index("[subs] alpha: chunk 1/2")
        beta_chunk_1 = output.index("[subs] beta: chunk 1/2")
        alpha_chunk_2 = output.index("[subs] alpha: chunk 2/2")
        beta_chunk_2 = output.index("[subs] beta: chunk 2/2")
        self.assertLess(alpha_chunk_1, beta_chunk_1)
        self.assertLess(beta_chunk_1, alpha_chunk_2)
        self.assertLess(alpha_chunk_2, beta_chunk_2)

    def test_run_legacy_sync_sources_round_robins_metadata_chunks_by_source(self):
        alpha_root = self.workspace_root / "alpha_meta_rr"
        beta_root = self.workspace_root / "beta_meta_rr"
        alpha_root.mkdir(parents=True, exist_ok=True)
        beta_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "alpha"
platform = "tiktok"
url = "https://www.tiktok.com/@alpha"
enabled = true
data_dir = "{alpha_root}"

[[sources]]
id = "beta"
platform = "tiktok"
url = "https://www.tiktok.com/@beta"
enabled = true
data_dir = "{beta_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        by_id = {source.id: source for source in sources}
        alpha = by_id["alpha"]
        beta = by_id["beta"]

        for source in (alpha, beta):
            source.media_archive.parent.mkdir(parents=True, exist_ok=True)
            source.media_archive.write_text(
                "\n".join(
                    f"tiktok 76188888888888888{index:02d}{0 if source.id == 'alpha' else 1}"
                    for index in range(1, 7)
                )
                + "\n",
                encoding="utf-8",
            )

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)

            def fake_run_command(command, dry_run=False):
                self.assertFalse(dry_run)
                args = list(command)
                template = str(args[args.index("-o") + 1])
                urls_path = Path(args[args.index("-a") + 1])
                for url in urls_path.read_text(encoding="utf-8").splitlines():
                    video_id = url.rstrip("/").rsplit("/", 1)[-1]
                    meta_path = Path(
                        template
                        .replace("%(id)s", video_id)
                        .replace("%(ext)s", "info.json")
                    )
                    meta_path.parent.mkdir(parents=True, exist_ok=True)
                    meta_path.write_text("{}", encoding="utf-8")
                return (0, "")

            stdout_capture = io.StringIO()
            with (
                mock.patch.object(self.mod, "run_command_with_output", side_effect=fake_run_command),
                redirect_stdout(stdout_capture),
            ):
                self.mod.run_legacy_sync_sources(
                    sources=[alpha, beta],
                    dry_run=False,
                    skip_media=True,
                    skip_subs=True,
                    skip_meta=False,
                    connection=connection,
                    metered_media_mode="off",
                    metered_min_archive_ids=0,
                    metered_playlist_end=5,
                )
        finally:
            connection.close()

        output = stdout_capture.getvalue()
        alpha_chunk_1 = output.index("[meta] alpha: chunk 1/2")
        beta_chunk_1 = output.index("[meta] beta: chunk 1/2")
        alpha_chunk_2 = output.index("[meta] alpha: chunk 2/2")
        beta_chunk_2 = output.index("[meta] beta: chunk 2/2")
        self.assertLess(alpha_chunk_1, beta_chunk_1)
        self.assertLess(beta_chunk_1, alpha_chunk_2)
        self.assertLess(alpha_chunk_2, beta_chunk_2)

    def test_run_legacy_sync_sources_applies_global_subtitle_limit(self):
        alpha_root = self.workspace_root / "alpha_subs_limit"
        beta_root = self.workspace_root / "beta_subs_limit"
        alpha_root.mkdir(parents=True, exist_ok=True)
        beta_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "alpha"
platform = "tiktok"
url = "https://www.tiktok.com/@alpha"
enabled = true
data_dir = "{alpha_root}"

[[sources]]
id = "beta"
platform = "tiktok"
url = "https://www.tiktok.com/@beta"
enabled = true
data_dir = "{beta_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        by_id = {source.id: source for source in sources}
        alpha = by_id["alpha"]
        beta = by_id["beta"]

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            now_iso = "2026-03-09T00:00:00+00:00"
            for source in (alpha, beta):
                for index in range(1, 7):
                    video_id = f"76171111111111111{index:02d}{0 if source.id == 'alpha' else 1}"
                    connection.execute(
                        """
                        INSERT INTO videos(source_id, video_id, media_path, has_media, has_subtitles, synced_at)
                        VALUES (?, ?, ?, 1, 0, ?)
                        """,
                        (source.id, video_id, str(source.media_dir / f"{video_id}.mp4"), now_iso),
                    )
            connection.commit()

            def fake_run_command(command, dry_run=False):
                self.assertFalse(dry_run)
                args = list(command)
                template = str(args[args.index("-o") + 1])
                urls_path = Path(args[args.index("-a") + 1])
                for url in urls_path.read_text(encoding="utf-8").splitlines():
                    video_id = url.rstrip("/").rsplit("/", 1)[-1]
                    subtitle_path = Path(
                        template
                        .replace("%(id)s", video_id)
                        .replace("%(language)s", "eng-US")
                        .replace("%(ext)s", "vtt")
                    )
                    subtitle_path.parent.mkdir(parents=True, exist_ok=True)
                    subtitle_path.write_text(
                        "WEBVTT\n\n00:00.000 --> 00:01.000\nhello\n",
                        encoding="utf-8",
                    )
                return (0, "")

            with mock.patch.object(self.mod, "run_command_with_output", side_effect=fake_run_command):
                self.mod.run_legacy_sync_sources(
                    sources=[alpha, beta],
                    dry_run=False,
                    skip_media=True,
                    skip_subs=False,
                    skip_meta=True,
                    connection=connection,
                    metered_media_mode="off",
                    metered_min_archive_ids=0,
                    metered_playlist_end=5,
                    limit=7,
                )
        finally:
            connection.close()

        alpha_subs = sorted(alpha.subs_dir.glob("*.vtt"))
        beta_subs = sorted(beta.subs_dir.glob("*.vtt"))
        self.assertEqual(len(alpha_subs), 6)
        self.assertEqual(len(beta_subs), 1)

    def test_run_legacy_sync_sources_applies_global_metadata_limit(self):
        alpha_root = self.workspace_root / "alpha_meta_limit"
        beta_root = self.workspace_root / "beta_meta_limit"
        alpha_root.mkdir(parents=True, exist_ok=True)
        beta_root.mkdir(parents=True, exist_ok=True)
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            f"""
[global]
ledger_db = "{self.db_path}"
ledger_csv = "{self.workspace_root / 'data' / 'master_ledger.csv'}"

[[sources]]
id = "alpha"
platform = "tiktok"
url = "https://www.tiktok.com/@alpha"
enabled = true
data_dir = "{alpha_root}"

[[sources]]
id = "beta"
platform = "tiktok"
url = "https://www.tiktok.com/@beta"
enabled = true
data_dir = "{beta_root}"
            """.strip()
            + "\n",
            encoding="utf-8",
        )
        _, sources = self.mod.load_config(config_path)
        by_id = {source.id: source for source in sources}
        alpha = by_id["alpha"]
        beta = by_id["beta"]

        for source in (alpha, beta):
            source.media_archive.parent.mkdir(parents=True, exist_ok=True)
            source.media_archive.write_text(
                "\n".join(
                    f"tiktok 76172222222222222{index:02d}{0 if source.id == 'alpha' else 1}"
                    for index in range(1, 7)
                )
                + "\n",
                encoding="utf-8",
            )

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)

            def fake_run_command(command, dry_run=False):
                self.assertFalse(dry_run)
                args = list(command)
                template = str(args[args.index("-o") + 1])
                urls_path = Path(args[args.index("-a") + 1])
                for url in urls_path.read_text(encoding="utf-8").splitlines():
                    video_id = url.rstrip("/").rsplit("/", 1)[-1]
                    meta_path = Path(
                        template
                        .replace("%(id)s", video_id)
                        .replace("%(ext)s", "info.json")
                    )
                    meta_path.parent.mkdir(parents=True, exist_ok=True)
                    meta_path.write_text("{}", encoding="utf-8")
                return (0, "")

            with mock.patch.object(self.mod, "run_command_with_output", side_effect=fake_run_command):
                self.mod.run_legacy_sync_sources(
                    sources=[alpha, beta],
                    dry_run=False,
                    skip_media=True,
                    skip_subs=True,
                    skip_meta=False,
                    connection=connection,
                    metered_media_mode="off",
                    metered_min_archive_ids=0,
                    metered_playlist_end=5,
                    limit=7,
                )
        finally:
            connection.close()

        alpha_meta = sorted(alpha.meta_dir.glob("*.info.json"))
        beta_meta = sorted(beta.meta_dir.glob("*.info.json"))
        self.assertEqual(len(alpha_meta), 6)
        self.assertEqual(len(beta_meta), 1)

    def test_sync_main_treats_zero_limit_as_unbounded(self):
        source_root = self.workspace_root / "sync_zero_limit"
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

        with (
            mock.patch.object(
                self.mod.sys,
                "argv",
                [
                    "substudy.py",
                    "sync",
                    "--config",
                    str(config_path),
                    "--dry-run",
                    "--skip-media",
                    "--skip-meta",
                ],
            ),
            mock.patch.object(self.mod, "run_legacy_sync_sources") as run_sync_mock,
        ):
            result = self.mod.main()

        self.assertEqual(result, 0)
        self.assertEqual(run_sync_mock.call_args.kwargs["limit"], None)

    def test_sync_source_skips_retry_subtitle_when_local_file_exists(self):
        source_root = self.workspace_root / "storiesofcz_subs_retry_existing"
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
        video_id = "7617999999999999999"
        source.subs_dir.mkdir(parents=True, exist_ok=True)
        (source.subs_dir / f"{video_id}.eng-US.vtt").write_text(
            "WEBVTT\n\n00:00.000 --> 00:01.000\nhello\n",
            encoding="utf-8",
        )

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            failed_at = "2026-03-08T00:00:00+00:00"
            self.mod.upsert_download_state(
                connection=connection,
                source_id=source.id,
                stage="subs",
                video_id=video_id,
                status="error",
                run_id=None,
                attempt_at=failed_at,
                url=self.mod.build_video_url(source, video_id),
                last_error="subtitle file missing after download attempt",
                retry_count=2,
                next_retry_at=None,
            )
            connection.commit()

            with mock.patch.object(self.mod, "run_command_with_output") as run_command_mock:
                self.mod.sync_source(
                    source=source,
                    dry_run=False,
                    skip_media=True,
                    skip_subs=False,
                    skip_meta=True,
                    connection=connection,
                )

            run_command_mock.assert_not_called()
            row = connection.execute(
                """
                SELECT status, retry_count, last_error, next_retry_at
                FROM download_state
                WHERE source_id = ? AND stage = 'subs' AND video_id = ?
                """,
                (source.id, video_id),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row[0]), "success")
            self.assertEqual(int(row[1]), 0)
            self.assertIsNone(row[2])
            self.assertIsNone(row[3])
        finally:
            connection.close()

    def test_prepare_subtitle_download_plan_upstream_override_ignores_english_and_generated_tracks(self):
        source_root = self.workspace_root / "storiesofcz_upstream_ja_only"
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
        source = self.mod.apply_upstream_sub_langs_override(
            sources,
            "ja.*,ja,jp.*,jpn.*",
        )[0]
        video_id = "7617444444444444444"
        source.subs_dir.mkdir(parents=True, exist_ok=True)
        (source.subs_dir / f"{video_id}.eng-US.vtt").write_text(
            "WEBVTT\n\n00:00.000 --> 00:01.000\nhello\n",
            encoding="utf-8",
        )
        (source.subs_dir / f"{video_id}.ja-local.vtt").write_text(
            "WEBVTT\n\n00:00.000 --> 00:01.000\nこんにちは\n",
            encoding="utf-8",
        )

        plan = self.mod.prepare_subtitle_download_plan(
            source=source,
            dry_run=False,
            connection=None,
            metadata_candidate_ids=None,
            new_media_ids=[video_id],
            strict_candidate_scope=False,
            active_urls_file=source.urls_file,
            cookie_flags=[],
            impersonate_flags=[],
            safe_video_url=lambda candidate_video_id: self.mod.build_video_url(source, candidate_video_id),
            source_cooldown_active=False,
            source_cooldown_until=None,
        )

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.target_ids, [video_id])
        self.assertIsNotNone(plan.command_template)
        assert plan.command_template is not None
        self.assertNotIn("--download-archive", plan.command_template)
        self.assertIn("--sub-langs", plan.command_template)
        sub_langs_index = plan.command_template.index("--sub-langs") + 1
        self.assertEqual(plan.command_template[sub_langs_index], "ja.*,ja,jp.*,jpn.*")

    def test_prepare_subtitle_download_plan_upstream_override_skips_when_upstream_match_exists(self):
        source_root = self.workspace_root / "storiesofcz_upstream_ja_existing"
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
        source = self.mod.apply_upstream_sub_langs_override(
            sources,
            "ja.*,ja,jp.*,jpn.*",
        )[0]
        video_id = "7617555555555555555"
        source.subs_dir.mkdir(parents=True, exist_ok=True)
        (source.subs_dir / f"{video_id}.ja-JP.vtt").write_text(
            "WEBVTT\n\n00:00.000 --> 00:01.000\nこんにちは\n",
            encoding="utf-8",
        )

        plan = self.mod.prepare_subtitle_download_plan(
            source=source,
            dry_run=False,
            connection=None,
            metadata_candidate_ids=None,
            new_media_ids=[video_id],
            strict_candidate_scope=False,
            active_urls_file=source.urls_file,
            cookie_flags=[],
            impersonate_flags=[],
            safe_video_url=lambda candidate_video_id: self.mod.build_video_url(source, candidate_video_id),
            source_cooldown_active=False,
            source_cooldown_until=None,
        )

        self.assertIsNone(plan)

    def test_prepare_subtitle_download_plan_upstream_override_bootstraps_english_only_video(self):
        source_root = self.workspace_root / "storiesofcz_upstream_ja_bootstrap"
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
        source = self.mod.apply_upstream_sub_langs_override(
            sources,
            "ja.*,ja,jp.*,jpn.*",
        )[0]
        video_id = "7617666666666666666"
        source.subs_dir.mkdir(parents=True, exist_ok=True)
        source.media_dir.mkdir(parents=True, exist_ok=True)
        (source.subs_dir / f"{video_id}.eng-US.vtt").write_text(
            "WEBVTT\n\n00:00.000 --> 00:01.000\nhello\n",
            encoding="utf-8",
        )
        (source.subs_dir / f"{video_id}.ja-local.vtt").write_text(
            "WEBVTT\n\n00:00.000 --> 00:01.000\nこんにちは\n",
            encoding="utf-8",
        )

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            connection.execute(
                """
                INSERT INTO videos(source_id, video_id, media_path, has_media, has_subtitles, synced_at)
                VALUES (?, ?, ?, 1, 1, ?)
                """,
                (
                    source.id,
                    video_id,
                    str(source.media_dir / f"{video_id}.mp4"),
                    "2026-03-09T00:00:00+00:00",
                ),
            )
            connection.commit()

            plan = self.mod.prepare_subtitle_download_plan(
                source=source,
                dry_run=False,
                connection=connection,
                metadata_candidate_ids=None,
                new_media_ids=[],
                strict_candidate_scope=False,
                active_urls_file=source.urls_file,
                cookie_flags=[],
                impersonate_flags=[],
                safe_video_url=lambda candidate_video_id: self.mod.build_video_url(source, candidate_video_id),
                source_cooldown_active=False,
                source_cooldown_until=None,
                limit=10,
            )
        finally:
            connection.close()

        self.assertIsNotNone(plan)
        assert plan is not None
        self.assertEqual(plan.target_ids, [video_id])
        self.assertEqual(int(plan.payload.get("bootstrap_count", 0)), 1)

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

            with mock.patch.object(self.mod, "run_command_with_output") as run_command_mock:
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
            with mock.patch.object(self.mod, "run_command_with_output") as run_command_mock:
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
            with mock.patch.object(self.mod, "run_command_with_output", return_value=(0, "")) as run_command_mock:
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

    def test_sync_source_blocked_media_skips_following_metadata(self):
        source_root = self.workspace_root / "storiesofcz_blocked_media_skip_meta"
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
            "tiktok 7618888888888888888\n",
            encoding="utf-8",
        )

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            commands: list[list[str]] = []

            def fake_run_command_with_output(command, dry_run):
                commands.append(list(command))
                return (
                    1,
                    "ERROR: [TikTok] Your IP address is blocked from accessing this post",
                )

            with mock.patch.object(
                self.mod,
                "run_command_with_output",
                side_effect=fake_run_command_with_output,
            ):
                self.mod.sync_source(
                    source=source,
                    dry_run=False,
                    skip_media=False,
                    skip_subs=True,
                    skip_meta=False,
                    connection=connection,
                )

            self.assertEqual(len(commands), 1)
            row = connection.execute(
                """
                SELECT blocked_until, last_error
                FROM source_access_state
                WHERE source_id = ?
                """,
                (source.id,),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertIsNotNone(row[0])
            self.assertIn("blocked", str(row[1]).lower())
        finally:
            connection.close()

    def test_sync_source_meta_chunk_failure_only_marks_attempted_targets(self):
        source_root = self.workspace_root / "storiesofcz_meta_chunk_failure"
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
        video_ids = [
            "7611111111111111111",
            "7611111111111111112",
            "7611111111111111113",
            "7611111111111111114",
            "7611111111111111115",
            "7611111111111111116",
        ]
        source.media_archive.parent.mkdir(parents=True, exist_ok=True)
        source.media_archive.write_text(
            "".join(f"tiktok {video_id}\n" for video_id in video_ids),
            encoding="utf-8",
        )

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            commands: list[list[str]] = []

            def fake_run_command_with_output(command, dry_run):
                commands.append(list(command))
                return (
                    1,
                    (
                        f"ERROR: [TikTok] {video_ids[0]}: "
                        "Unable to extract universal data for rehydration"
                    ),
                )

            with mock.patch.object(
                self.mod,
                "run_command_with_output",
                side_effect=fake_run_command_with_output,
            ):
                self.mod.sync_source(
                    source=source,
                    dry_run=False,
                    skip_media=True,
                    skip_subs=True,
                    skip_meta=False,
                    connection=connection,
                )

            self.assertEqual(len(commands), 2)

            state_rows = connection.execute(
                """
                SELECT video_id, status, last_error
                FROM download_state
                WHERE source_id = ? AND stage = 'meta'
                ORDER BY video_id
                """,
                (source.id,),
            ).fetchall()
            self.assertEqual(len(state_rows), 5)
            self.assertEqual({str(row[0]) for row in state_rows}, set(video_ids[:5]))
            for row in state_rows:
                self.assertEqual(str(row[1]), "error")
                self.assertIn("rehydration", str(row[2]).lower())

            missing_row = connection.execute(
                """
                SELECT status
                FROM download_state
                WHERE source_id = ? AND stage = 'meta' AND video_id = ?
                """,
                (source.id, video_ids[-1]),
            ).fetchone()
            self.assertIsNone(missing_row)

            run_row = connection.execute(
                """
                SELECT status, target_count, success_count, failure_count, error_message
                FROM download_runs
                WHERE source_id = ? AND stage = 'meta'
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (source.id,),
            ).fetchone()
            self.assertIsNotNone(run_row)
            self.assertEqual(str(run_row[0]), "error")
            self.assertEqual(int(run_row[1] or 0), len(video_ids))
            self.assertEqual(int(run_row[2] or 0), 0)
            self.assertEqual(int(run_row[3] or 0), 5)
            self.assertIn("rehydration", str(run_row[4]).lower())

            access_row = connection.execute(
                """
                SELECT blocked_until, last_error
                FROM source_access_state
                WHERE source_id = ?
                """,
                (source.id,),
            ).fetchone()
            self.assertIsNotNone(access_row)
            self.assertIsNone(access_row[0])
            self.assertIn("rehydration", str(access_row[1]).lower())
        finally:
            connection.close()

    def test_sync_source_meta_chunk_transient_breaker_stops_after_two_failed_chunks(self):
        source_root = self.workspace_root / "storiesofcz_meta_chunk_transient_breaker"
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
        video_ids = [
            "7611111111111111111",
            "7611111111111111112",
            "7611111111111111113",
            "7611111111111111114",
            "7611111111111111115",
            "7611111111111111116",
            "7611111111111111117",
            "7611111111111111118",
            "7611111111111111119",
            "7611111111111111120",
            "7611111111111111121",
        ]
        source.media_archive.parent.mkdir(parents=True, exist_ok=True)
        source.media_archive.write_text(
            "".join(f"tiktok {video_id}\n" for video_id in video_ids),
            encoding="utf-8",
        )

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            commands: list[list[str]] = []

            def fake_run_command_with_output(command, dry_run):
                commands.append(list(command))
                urls_file = command[command.index("-a") + 1]
                chunk_urls = Path(urls_file).read_text(encoding="utf-8").splitlines()
                chunk_video_ids = [
                    str(url).rstrip("/").split("/")[-1]
                    for url in chunk_urls
                    if str(url).strip()
                ]
                output_lines = [
                    (
                        f"ERROR: [TikTok] {video_id}: "
                        "Unable to extract universal data for rehydration"
                    )
                    for video_id in chunk_video_ids
                ]
                return (0, "\n".join(output_lines))

            with mock.patch.object(
                self.mod,
                "run_command_with_output",
                side_effect=fake_run_command_with_output,
            ):
                self.mod.sync_source(
                    source=source,
                    dry_run=False,
                    skip_media=True,
                    skip_subs=True,
                    skip_meta=False,
                    connection=connection,
                )

            self.assertEqual(len(commands), 4)

            state_rows = connection.execute(
                """
                SELECT video_id, status, last_error
                FROM download_state
                WHERE source_id = ? AND stage = 'meta'
                ORDER BY video_id
                """,
                (source.id,),
            ).fetchall()
            self.assertEqual(len(state_rows), 10)
            self.assertEqual({str(row[0]) for row in state_rows}, set(video_ids[:10]))
            for row in state_rows:
                self.assertEqual(str(row[1]), "error")
                self.assertIn("rehydration", str(row[2]).lower())

            missing_row = connection.execute(
                """
                SELECT status
                FROM download_state
                WHERE source_id = ? AND stage = 'meta' AND video_id = ?
                """,
                (source.id, video_ids[-1]),
            ).fetchone()
            self.assertIsNone(missing_row)

            run_row = connection.execute(
                """
                SELECT status, success_count, failure_count, error_message
                FROM download_runs
                WHERE source_id = ? AND stage = 'meta'
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (source.id,),
            ).fetchone()
            self.assertIsNotNone(run_row)
            self.assertEqual(str(run_row[0]), "error")
            self.assertEqual(int(run_row[1] or 0), 0)
            self.assertEqual(int(run_row[2] or 0), 10)
            self.assertIn("circuit breaker", str(run_row[3]).lower())
        finally:
            connection.close()

    def test_sync_source_meta_chunk_transient_breaker_stops_after_high_rate_failed_chunks(self):
        source_root = self.workspace_root / "storiesofcz_meta_chunk_transient_rate_breaker"
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
        video_ids = [
            "7621111111111111111",
            "7621111111111111112",
            "7621111111111111113",
            "7621111111111111114",
            "7621111111111111115",
            "7621111111111111116",
            "7621111111111111117",
            "7621111111111111118",
            "7621111111111111119",
            "7621111111111111120",
            "7621111111111111121",
        ]
        source.media_archive.parent.mkdir(parents=True, exist_ok=True)
        source.media_archive.write_text(
            "".join(f"tiktok {video_id}\n" for video_id in video_ids),
            encoding="utf-8",
        )

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            commands: list[list[str]] = []

            def fake_run_command_with_output(command, dry_run):
                commands.append(list(command))
                urls_file = command[command.index("-a") + 1]
                chunk_urls = Path(urls_file).read_text(encoding="utf-8").splitlines()
                chunk_video_ids = [
                    str(url).rstrip("/").split("/")[-1]
                    for url in chunk_urls
                    if str(url).strip()
                ]
                transient_ids = chunk_video_ids[:3]
                output_lines = [
                    (
                        f"ERROR: [TikTok] {video_id}: "
                        "Unable to extract universal data for rehydration"
                    )
                    for video_id in transient_ids
                ]
                return (0, "\n".join(output_lines))

            with mock.patch.object(
                self.mod,
                "run_command_with_output",
                side_effect=fake_run_command_with_output,
            ):
                self.mod.sync_source(
                    source=source,
                    dry_run=False,
                    skip_media=True,
                    skip_subs=True,
                    skip_meta=False,
                    connection=connection,
                )

            self.assertEqual(len(commands), 4)

            state_rows = connection.execute(
                """
                SELECT video_id, last_error
                FROM download_state
                WHERE source_id = ? AND stage = 'meta'
                ORDER BY video_id
                """,
                (source.id,),
            ).fetchall()
            self.assertEqual(len(state_rows), 10)
            self.assertTrue(
                any("rehydration" in str(row[1]).lower() for row in state_rows)
            )
            self.assertTrue(
                any("missing after download attempt" in str(row[1]).lower() for row in state_rows)
            )

            run_row = connection.execute(
                """
                SELECT failure_count, error_message
                FROM download_runs
                WHERE source_id = ? AND stage = 'meta'
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (source.id,),
            ).fetchone()
            self.assertIsNotNone(run_row)
            self.assertEqual(int(run_row[0] or 0), 10)
            self.assertIn("circuit breaker", str(run_row[1]).lower())
            self.assertIn("transient", str(run_row[1]).lower())
        finally:
            connection.close()

    def test_sync_source_meta_chunk_blocked_error_stops_remaining_chunks_immediately(self):
        source_root = self.workspace_root / "storiesofcz_meta_chunk_blocked_breaker"
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
        video_ids = [
            "7631111111111111111",
            "7631111111111111112",
            "7631111111111111113",
            "7631111111111111114",
            "7631111111111111115",
            "7631111111111111116",
        ]
        source.media_archive.parent.mkdir(parents=True, exist_ok=True)
        source.media_archive.write_text(
            "".join(f"tiktok {video_id}\n" for video_id in video_ids),
            encoding="utf-8",
        )

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            commands: list[list[str]] = []

            def fake_run_command_with_output(command, dry_run):
                commands.append(list(command))
                urls_file = command[command.index("-a") + 1]
                chunk_urls = Path(urls_file).read_text(encoding="utf-8").splitlines()
                chunk_video_ids = [
                    str(url).rstrip("/").split("/")[-1]
                    for url in chunk_urls
                    if str(url).strip()
                ]
                return (
                    0,
                    (
                        f"ERROR: [TikTok] {chunk_video_ids[0]}: "
                        "Your IP address is blocked from accessing this post"
                    ),
                )

            with mock.patch.object(
                self.mod,
                "run_command_with_output",
                side_effect=fake_run_command_with_output,
            ):
                self.mod.sync_source(
                    source=source,
                    dry_run=False,
                    skip_media=True,
                    skip_subs=True,
                    skip_meta=False,
                    connection=connection,
                )

            self.assertEqual(len(commands), 1)

            run_row = connection.execute(
                """
                SELECT failure_count, error_message
                FROM download_runs
                WHERE source_id = ? AND stage = 'meta'
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (source.id,),
            ).fetchone()
            self.assertIsNotNone(run_row)
            self.assertEqual(int(run_row[0] or 0), 5)
            self.assertIn("blocked", str(run_row[1]).lower())

            blocked_row = connection.execute(
                """
                SELECT last_error
                FROM download_state
                WHERE source_id = ? AND stage = 'meta' AND video_id = ?
                """,
                (source.id, video_ids[0]),
            ).fetchone()
            self.assertIsNotNone(blocked_row)
            self.assertIn("blocked", str(blocked_row[0]).lower())

            unattempted_row = connection.execute(
                """
                SELECT status
                FROM download_state
                WHERE source_id = ? AND stage = 'meta' AND video_id = ?
                """,
                (source.id, video_ids[-1]),
            ).fetchone()
            self.assertIsNone(unattempted_row)
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

    def test_feed_and_toggle_preference_state(self):
        media_path = self.workspace_root / "storiesofcz.mp4"
        media_path.write_bytes(b"")
        now_iso = dt.datetime(2026, 3, 10, 0, 0, tzinfo=dt.timezone.utc).isoformat()

        connection = sqlite3.connect(str(self.db_path))
        try:
            connection.execute(
                """
                INSERT INTO videos(
                    source_id,
                    video_id,
                    title,
                    media_path,
                    has_media,
                    synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("storiesofcz", "7611111111111111111", "sample", str(media_path), 1, now_iso),
            )
            connection.commit()
        finally:
            connection.close()

        handler_class = self.mod.build_web_handler(
            db_path=self.db_path,
            static_dir=self.mod.WEB_STATIC_DIR,
            allowed_source_ids=set(),
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
        favorite_url = f"http://{host}:{port}/api/favorites/toggle"
        dislike_url = f"http://{host}:{port}/api/dislikes/toggle"
        not_interested_url = f"http://{host}:{port}/api/not-interested/toggle"
        toggle_body = json.dumps(
            {
                "source_id": "storiesofcz",
                "video_id": "7611111111111111111",
            }
        ).encode("utf-8")

        with urllib.request.urlopen(feed_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            payload = json.loads(response.read().decode("utf-8"))
        self.assertEqual(payload.get("count"), 1)
        self.assertFalse(payload["videos"][0]["is_favorite"])
        self.assertFalse(payload["videos"][0]["is_disliked"])
        self.assertFalse(payload["videos"][0]["is_not_interested"])

        not_interested_request = urllib.request.Request(
            not_interested_url,
            data=toggle_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(not_interested_request, timeout=5) as response:
            self.assertEqual(response.status, 200)
            not_interested_payload = json.loads(response.read().decode("utf-8"))
        self.assertTrue(not_interested_payload["is_not_interested"])
        self.assertFalse(not_interested_payload["is_favorite"])
        self.assertFalse(not_interested_payload["is_disliked"])

        dislike_request = urllib.request.Request(
            dislike_url,
            data=toggle_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(dislike_request, timeout=5) as response:
            self.assertEqual(response.status, 200)
            dislike_payload = json.loads(response.read().decode("utf-8"))
        self.assertTrue(dislike_payload["is_disliked"])
        self.assertFalse(dislike_payload["is_favorite"])
        self.assertFalse(dislike_payload["is_not_interested"])

        favorite_request = urllib.request.Request(
            favorite_url,
            data=toggle_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(favorite_request, timeout=5) as response:
            self.assertEqual(response.status, 200)
            favorite_payload = json.loads(response.read().decode("utf-8"))
        self.assertTrue(favorite_payload["is_favorite"])
        self.assertFalse(favorite_payload["is_disliked"])
        self.assertFalse(favorite_payload["is_not_interested"])

        with urllib.request.urlopen(feed_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            updated_payload = json.loads(response.read().decode("utf-8"))
        self.assertTrue(updated_payload["videos"][0]["is_favorite"])
        self.assertFalse(updated_payload["videos"][0]["is_disliked"])
        self.assertFalse(updated_payload["videos"][0]["is_not_interested"])

    def test_favorites_toggle_requires_application_json(self):
        media_path = self.workspace_root / "storiesofcz_csrf.mp4"
        media_path.write_bytes(b"")
        now_iso = dt.datetime(2026, 3, 10, 0, 0, tzinfo=dt.timezone.utc).isoformat()

        connection = sqlite3.connect(str(self.db_path))
        try:
            connection.execute(
                """
                INSERT INTO videos(
                    source_id,
                    video_id,
                    title,
                    media_path,
                    has_media,
                    synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("storiesofcz", "7611111111111111888", "csrf", str(media_path), 1, now_iso),
            )
            connection.commit()
        finally:
            connection.close()

        handler_class = self.mod.build_web_handler(
            db_path=self.db_path,
            static_dir=self.mod.WEB_STATIC_DIR,
            allowed_source_ids=set(),
            restrict_to_source_ids=False,
        )
        server = self.mod.ThreadingHTTPServer(("127.0.0.1", 0), handler_class)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(lambda: thread.join(timeout=3))

        host, port = server.server_address
        toggle_url = f"http://{host}:{port}/api/favorites/toggle"
        request = urllib.request.Request(
            toggle_url,
            data=b'{"source_id":"storiesofcz","video_id":"7611111111111111888"}',
            headers={"Content-Type": "text/plain"},
            method="POST",
        )
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(request, timeout=5)
        self.assertEqual(ctx.exception.code, 415)
        body = json.loads(ctx.exception.read().decode("utf-8"))
        self.assertEqual(body.get("error"), "Content-Type must be application/json.")
        ctx.exception.close()

        feed_url = f"http://{host}:{port}/api/feed?limit=20&offset=0"
        with urllib.request.urlopen(feed_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            payload = json.loads(response.read().decode("utf-8"))
        self.assertFalse(payload["videos"][0]["is_favorite"])

    def test_media_endpoint_serves_registered_video_file(self):
        media_path = self.workspace_root / "registered.mp4"
        media_bytes = b"registered-media"
        media_path.write_bytes(media_bytes)
        now_iso = dt.datetime(2026, 3, 10, 0, 0, tzinfo=dt.timezone.utc).isoformat()

        connection = sqlite3.connect(str(self.db_path))
        try:
            connection.execute(
                """
                INSERT INTO videos(
                    source_id,
                    video_id,
                    title,
                    media_path,
                    has_media,
                    synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("storiesofcz", "7611111111111111999", "registered", str(media_path), 1, now_iso),
            )
            connection.commit()
        finally:
            connection.close()

        handler_class = self.mod.build_web_handler(
            db_path=self.db_path,
            static_dir=self.mod.WEB_STATIC_DIR,
            allowed_source_ids=set(),
            restrict_to_source_ids=False,
        )
        server = self.mod.ThreadingHTTPServer(("127.0.0.1", 0), handler_class)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(lambda: thread.join(timeout=3))

        host, port = server.server_address
        media_url = f"http://{host}:{port}/media/{self.mod.encode_path_token(media_path)}"
        with urllib.request.urlopen(media_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.read(), media_bytes)

    def test_media_endpoint_rejects_unregistered_local_file(self):
        rogue_path = self.workspace_root / "rogue.txt"
        rogue_path.write_text("secret", encoding="utf-8")

        handler_class = self.mod.build_web_handler(
            db_path=self.db_path,
            static_dir=self.mod.WEB_STATIC_DIR,
            allowed_source_ids=set(),
            restrict_to_source_ids=False,
        )
        server = self.mod.ThreadingHTTPServer(("127.0.0.1", 0), handler_class)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(lambda: thread.join(timeout=3))

        host, port = server.server_address
        media_url = f"http://{host}:{port}/media/{self.mod.encode_path_token(rogue_path)}"
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(media_url, timeout=5)
        self.assertEqual(ctx.exception.code, 404)
        ctx.exception.close()

    def test_playback_stats_record_and_feed(self):
        media_path = self.workspace_root / "storiesofcz_stats.mp4"
        media_path.write_bytes(b"")
        now_iso = dt.datetime(2026, 3, 10, 0, 0, tzinfo=dt.timezone.utc).isoformat()

        connection = sqlite3.connect(str(self.db_path))
        try:
            connection.execute(
                """
                INSERT INTO videos(
                    source_id,
                    video_id,
                    title,
                    media_path,
                    has_media,
                    synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("storiesofcz", "7622222222222222222", "stats", str(media_path), 1, now_iso),
            )
            connection.execute(
                """
                INSERT INTO subtitle_bookmarks(
                    source_id,
                    video_id,
                    track,
                    start_ms,
                    end_ms,
                    text,
                    note,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "storiesofcz",
                    "7622222222222222222",
                    "en",
                    1000,
                    2400,
                    "cue bookmark",
                    "",
                    now_iso,
                ),
            )
            connection.execute(
                """
                INSERT INTO dictionary_bookmarks(
                    source_id,
                    video_id,
                    track,
                    cue_start_ms,
                    cue_end_ms,
                    cue_text,
                    dict_entry_id,
                    dict_source_name,
                    lookup_term,
                    term,
                    term_norm,
                    definition,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "storiesofcz",
                    "7622222222222222222",
                    "en",
                    1000,
                    2400,
                    "cue bookmark",
                    1,
                    "test-dict",
                    "alpha",
                    "Alpha",
                    "alpha",
                    "first meaning",
                    now_iso,
                    now_iso,
                ),
            )
            connection.execute(
                """
                INSERT INTO dictionary_bookmarks(
                    source_id,
                    video_id,
                    track,
                    cue_start_ms,
                    cue_end_ms,
                    cue_text,
                    dict_entry_id,
                    dict_source_name,
                    lookup_term,
                    term,
                    term_norm,
                    definition,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "storiesofcz",
                    "7622222222222222222",
                    "en",
                    3000,
                    4200,
                    "second cue",
                    2,
                    "test-dict",
                    "beta",
                    "Beta",
                    "beta",
                    "second meaning",
                    now_iso,
                    now_iso,
                ),
            )
            connection.commit()
        finally:
            connection.close()

        handler_class = self.mod.build_web_handler(
            db_path=self.db_path,
            static_dir=self.mod.WEB_STATIC_DIR,
            allowed_source_ids=set(),
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
        stats_url = f"http://{host}:{port}/api/playback-stats/record"

        stats_request_1 = urllib.request.Request(
            stats_url,
            data=json.dumps(
                {
                    "source_id": "storiesofcz",
                    "video_id": "7622222222222222222",
                    "impression_increment": 1,
                    "play_increment": 1,
                    "watch_seconds": 12.5,
                    "completed_increment": 0,
                    "fast_skip_increment": 0,
                    "shallow_skip_increment": 1,
                    "last_position_seconds": 13,
                }
            ).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(stats_request_1, timeout=5) as response:
            self.assertEqual(response.status, 200)
            payload_1 = json.loads(response.read().decode("utf-8"))
        self.assertEqual(payload_1["playback_stats"]["impression_count"], 1)
        self.assertEqual(payload_1["playback_stats"]["play_count"], 1)
        self.assertAlmostEqual(payload_1["playback_stats"]["total_watch_seconds"], 12.5, places=3)
        self.assertEqual(payload_1["playback_stats"]["completed_count"], 0)
        self.assertEqual(payload_1["playback_stats"]["fast_skip_count"], 0)
        self.assertEqual(payload_1["playback_stats"]["shallow_skip_count"], 1)
        self.assertEqual(payload_1["playback_stats"]["last_position_seconds"], 13.0)
        self.assertTrue(payload_1["playback_stats"]["last_served_at"])

        stats_request_2 = urllib.request.Request(
            stats_url,
            data=json.dumps(
                {
                    "source_id": "storiesofcz",
                    "video_id": "7622222222222222222",
                    "play_increment": 0,
                    "watch_seconds": 7.25,
                    "completed_increment": 1,
                    "fast_skip_increment": 1,
                    "shallow_skip_increment": 0,
                    "last_position_seconds": 61,
                }
            ).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(stats_request_2, timeout=5) as response:
            self.assertEqual(response.status, 200)
            payload_2 = json.loads(response.read().decode("utf-8"))
        self.assertEqual(payload_2["playback_stats"]["play_count"], 1)
        self.assertAlmostEqual(payload_2["playback_stats"]["total_watch_seconds"], 19.75, places=3)
        self.assertEqual(payload_2["playback_stats"]["completed_count"], 1)
        self.assertEqual(payload_2["playback_stats"]["impression_count"], 1)
        self.assertEqual(payload_2["playback_stats"]["fast_skip_count"], 1)
        self.assertEqual(payload_2["playback_stats"]["shallow_skip_count"], 1)
        self.assertEqual(payload_2["playback_stats"]["last_position_seconds"], 61.0)
        self.assertTrue(payload_2["playback_stats"]["last_played_at"])
        self.assertTrue(payload_2["playback_stats"]["last_completed_at"])

        with urllib.request.urlopen(feed_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            feed_payload = json.loads(response.read().decode("utf-8"))
        stats = feed_payload["videos"][0]["playback_stats"]
        self.assertEqual(feed_payload["videos"][0]["cue_bookmark_count"], 1)
        self.assertEqual(feed_payload["videos"][0]["dictionary_bookmark_count"], 2)
        self.assertEqual(feed_payload["videos"][0]["dictionary_bookmark_unique_term_count"], 2)
        self.assertEqual(stats["impression_count"], 1)
        self.assertEqual(stats["play_count"], 1)
        self.assertAlmostEqual(stats["total_watch_seconds"], 19.75, places=3)
        self.assertEqual(stats["completed_count"], 1)
        self.assertEqual(stats["fast_skip_count"], 1)
        self.assertEqual(stats["shallow_skip_count"], 1)
        self.assertEqual(stats["last_position_seconds"], 61.0)
        self.assertTrue(stats["last_played_at"])
        self.assertTrue(stats["last_served_at"])
        self.assertTrue(stats["last_completed_at"])

    def test_feed_translation_filter_supports_variants(self):
        now_iso = dt.datetime(2026, 3, 10, 0, 0, tzinfo=dt.timezone.utc).isoformat()
        upstream_video_id = "video-upstream"
        claude_video_id = "video-claude"
        local_video_id = "video-local"
        no_ja_video_id = "video-no-ja"

        connection = sqlite3.connect(str(self.db_path))
        try:
            def insert_video(video_id: str, media_name: str) -> Path:
                media_path = self.workspace_root / media_name
                media_path.write_bytes(b"")
                connection.execute(
                    """
                    INSERT INTO videos(
                        source_id,
                        video_id,
                        title,
                        media_path,
                        has_media,
                        synced_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ("storiesofcz", video_id, video_id, str(media_path), 1, now_iso),
                )
                return media_path

            insert_video(upstream_video_id, "video-upstream.mp4")
            upstream_subtitle_path = self.workspace_root / "video-upstream.NA.jpn-JP.vtt"
            upstream_subtitle_path.write_text("WEBVTT\n\n", encoding="utf-8")
            connection.execute(
                """
                INSERT INTO subtitles(source_id, video_id, language, subtitle_path, ext)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("storiesofcz", upstream_video_id, "NA.jpn-JP", str(upstream_subtitle_path), "vtt"),
            )

            insert_video(claude_video_id, "video-claude.mp4")
            claude_source_path = self.workspace_root / "video-claude.en.vtt"
            claude_source_path.write_text("WEBVTT\n\n", encoding="utf-8")
            claude_subtitle_path = self.workspace_root / "video-claude.ja.vtt"
            claude_subtitle_path.write_text("WEBVTT\n\n", encoding="utf-8")
            connection.execute(
                """
                INSERT INTO subtitles(source_id, video_id, language, subtitle_path, ext)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("storiesofcz", claude_video_id, "ja", str(claude_subtitle_path), "vtt"),
            )
            self.mod.record_translation_run(
                connection=connection,
                source_id="storiesofcz",
                video_id=claude_video_id,
                source_path=claude_source_path,
                output_path=claude_subtitle_path,
                cue_count=2,
                cue_match=True,
                agent="claude-opus-4-6",
                method="manual-import",
                method_version="manual-import-v1",
                summary="claude generated ja subtitle",
                source_lang="en",
                target_lang="ja",
                status="active",
                started_at=now_iso,
                finished_at=now_iso,
            )

            insert_video(local_video_id, "video-local.mp4")
            local_source_path = self.workspace_root / "video-local.en.vtt"
            local_source_path.write_text("WEBVTT\n\n", encoding="utf-8")
            local_subtitle_path = self.workspace_root / "video-local.ja.vtt"
            local_subtitle_path.write_text("WEBVTT\n\n", encoding="utf-8")
            connection.execute(
                """
                INSERT INTO subtitles(source_id, video_id, language, subtitle_path, ext)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("storiesofcz", local_video_id, "ja", str(local_subtitle_path), "vtt"),
            )
            self.mod.record_translation_run(
                connection=connection,
                source_id="storiesofcz",
                video_id=local_video_id,
                source_path=local_source_path,
                output_path=local_subtitle_path,
                cue_count=2,
                cue_match=True,
                agent="local-llm",
                method="multi-stage",
                method_version="local-v1",
                summary="local generated ja subtitle",
                source_lang="en",
                target_lang="ja",
                status="active",
                started_at=now_iso,
                finished_at=now_iso,
            )

            insert_video(no_ja_video_id, "video-no-ja.mp4")
            self.mod.ensure_subtitles_origin_columns(connection)
            connection.commit()
        finally:
            connection.close()

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
url = "https://example.com/@storiesofcz"
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

        def fetch_feed_payload(filter_value: str) -> dict[str, Any]:
            url = f"http://{host}:{port}/api/feed?limit=20&offset=0&translation_filter={urllib.parse.quote(filter_value)}"
            with urllib.request.urlopen(url, timeout=5) as response:
                self.assertEqual(response.status, 200)
                return json.loads(response.read().decode("utf-8"))

        payload = fetch_feed_payload("ja_only")
        self.assertEqual(str(payload.get("translation_filter") or ""), "ja_only")
        self.assertEqual(
            {str(video.get("video_id") or "") for video in payload.get("videos", [])},
            {upstream_video_id, claude_video_id, local_video_id},
        )
        self.assertEqual(payload.get("sources"), ["storiesofcz"])

        payload = fetch_feed_payload("upstream")
        self.assertEqual(str(payload.get("translation_filter") or ""), "upstream")
        self.assertEqual(
            [str(video.get("video_id") or "") for video in payload.get("videos", [])],
            [upstream_video_id],
        )
        self.assertEqual(payload.get("sources"), ["storiesofcz"])

        payload = fetch_feed_payload("claude")
        self.assertEqual(str(payload.get("translation_filter") or ""), "claude")
        self.assertEqual(
            [str(video.get("video_id") or "") for video in payload.get("videos", [])],
            [claude_video_id],
        )
        self.assertEqual(payload.get("sources"), ["storiesofcz"])

        payload = fetch_feed_payload("local")
        self.assertEqual(str(payload.get("translation_filter") or ""), "local")
        self.assertEqual(
            [str(video.get("video_id") or "") for video in payload.get("videos", [])],
            [local_video_id],
        )
        self.assertEqual(payload.get("sources"), ["storiesofcz"])

        payload = fetch_feed_payload("ja_missing")
        self.assertEqual(str(payload.get("translation_filter") or ""), "ja_missing")
        self.assertEqual(
            [str(video.get("video_id") or "") for video in payload.get("videos", [])],
            [no_ja_video_id],
        )
        self.assertEqual(payload.get("sources"), ["storiesofcz"])

    def test_feed_source_filter_limits_reported_sources(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"

[[sources]]
id = "alpha"
platform = "tiktok"
url = "https://example.com/@alpha"
enabled = true

[[sources]]
id = "beta"
platform = "tiktok"
url = "https://example.com/@beta"
enabled = true
            """.strip()
            + "\n",
            encoding="utf-8",
        )

        now_iso = dt.datetime(2026, 3, 10, 0, 0, tzinfo=dt.timezone.utc).isoformat()
        connection = sqlite3.connect(str(self.db_path))
        try:
            for source_id, video_id in (("alpha", "video-alpha"), ("beta", "video-beta")):
                media_path = self.workspace_root / f"{video_id}.mp4"
                media_path.write_bytes(b"")
                connection.execute(
                    """
                    INSERT INTO videos(
                        source_id,
                        video_id,
                        title,
                        media_path,
                        has_media,
                        synced_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (source_id, video_id, video_id, str(media_path), 1, now_iso),
                )
            connection.commit()
        finally:
            connection.close()

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
        feed_url = f"http://{host}:{port}/api/feed?limit=20&offset=0&source_id=alpha"
        with urllib.request.urlopen(feed_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            payload = json.loads(response.read().decode("utf-8"))

        self.assertEqual([video["source_id"] for video in payload.get("videos", [])], ["alpha"])
        self.assertEqual(payload.get("sources"), ["alpha"])

    def test_feed_translation_filters_ignore_stale_subtitle_rows(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"

[[sources]]
id = "alpha"
platform = "tiktok"
url = "https://example.com/@alpha"
enabled = true
            """.strip()
            + "\n",
            encoding="utf-8",
        )

        now_iso = dt.datetime(2026, 3, 10, 0, 0, tzinfo=dt.timezone.utc).isoformat()
        media_path = self.workspace_root / "video-stale.mp4"
        media_path.write_bytes(b"")
        stale_subtitle_path = self.workspace_root / "video-stale.ja.vtt"

        connection = sqlite3.connect(str(self.db_path))
        try:
            connection.execute(
                """
                INSERT INTO videos(
                    source_id,
                    video_id,
                    title,
                    media_path,
                    has_media,
                    synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alpha", "video-stale", "video-stale", str(media_path), 1, now_iso),
            )
            connection.execute(
                """
                INSERT INTO subtitles(source_id, video_id, language, subtitle_path, ext)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("alpha", "video-stale", "ja", str(stale_subtitle_path), "vtt"),
            )
            self.mod.ensure_subtitles_origin_columns(connection)
            connection.commit()
        finally:
            connection.close()

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

        def fetch_payload(filter_value: str) -> dict[str, Any]:
            url = (
                f"http://{host}:{port}/api/feed?limit=20&offset=0"
                f"&translation_filter={urllib.parse.quote(filter_value)}"
            )
            with urllib.request.urlopen(url, timeout=5) as response:
                self.assertEqual(response.status, 200)
                return json.loads(response.read().decode("utf-8"))

        payload = fetch_payload("ja_missing")
        self.assertEqual(
            [str(video.get("video_id") or "") for video in payload.get("videos", [])],
            ["video-stale"],
        )
        self.assertEqual(payload.get("sources"), ["alpha"])

        payload = fetch_payload("ja_only")
        self.assertEqual(payload.get("videos"), [])
        self.assertEqual(payload.get("sources"), [])

    def test_feed_source_filter_omits_sources_when_translation_filter_matches_nothing(self):
        config_dir = self.workspace_root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / "sources.toml"
        config_path.write_text(
            """
[global]
ledger_db = "data/master_ledger.sqlite"
ledger_csv = "data/master_ledger.csv"

[[sources]]
id = "alpha"
platform = "tiktok"
url = "https://example.com/@alpha"
enabled = true
            """.strip()
            + "\n",
            encoding="utf-8",
        )

        now_iso = dt.datetime(2026, 3, 10, 0, 0, tzinfo=dt.timezone.utc).isoformat()
        media_path = self.workspace_root / "video-local-only.mp4"
        media_path.write_bytes(b"")
        source_subtitle_path = self.workspace_root / "video-local-only.en.vtt"
        source_subtitle_path.write_text("WEBVTT\n\n", encoding="utf-8")
        local_subtitle_path = self.workspace_root / "video-local-only.ja.vtt"
        local_subtitle_path.write_text("WEBVTT\n\n", encoding="utf-8")

        connection = sqlite3.connect(str(self.db_path))
        try:
            connection.execute(
                """
                INSERT INTO videos(
                    source_id,
                    video_id,
                    title,
                    media_path,
                    has_media,
                    synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("alpha", "video-local-only", "video-local-only", str(media_path), 1, now_iso),
            )
            connection.execute(
                """
                INSERT INTO subtitles(source_id, video_id, language, subtitle_path, ext)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("alpha", "video-local-only", "ja", str(local_subtitle_path), "vtt"),
            )
            self.mod.record_translation_run(
                connection=connection,
                source_id="alpha",
                video_id="video-local-only",
                source_path=source_subtitle_path,
                output_path=local_subtitle_path,
                cue_count=1,
                cue_match=True,
                agent="local-llm",
                method="multi-stage",
                method_version="local-v1",
                summary="local subtitle only",
                source_lang="en",
                target_lang="ja",
                status="active",
                started_at=now_iso,
                finished_at=now_iso,
            )
            self.mod.ensure_subtitles_origin_columns(connection)
            connection.commit()
        finally:
            connection.close()

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
        feed_url = (
            f"http://{host}:{port}/api/feed?limit=20&offset=0"
            f"&source_id=alpha&translation_filter=upstream"
        )
        with urllib.request.urlopen(feed_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            payload = json.loads(response.read().decode("utf-8"))

        self.assertEqual(payload.get("videos"), [])
        self.assertEqual(payload.get("sources"), [])

    def test_feed_pagination_skips_rows_with_missing_media_files(self):
        now_iso = dt.datetime(2026, 3, 10, 0, 0, tzinfo=dt.timezone.utc).isoformat()
        missing_media_path = self.workspace_root / "missing.mp4"
        valid_media_path = self.workspace_root / "valid.mp4"
        valid_media_path.write_bytes(b"ok")

        connection = sqlite3.connect(str(self.db_path))
        try:
            connection.execute(
                """
                INSERT INTO videos(
                    source_id,
                    video_id,
                    title,
                    media_path,
                    has_media,
                    synced_at,
                    upload_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("storiesofcz", "video-missing", "missing", str(missing_media_path), 1, now_iso, "20260311"),
            )
            connection.execute(
                """
                INSERT INTO videos(
                    source_id,
                    video_id,
                    title,
                    media_path,
                    has_media,
                    synced_at,
                    upload_date
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("storiesofcz", "video-valid", "valid", str(valid_media_path), 1, now_iso, "20260310"),
            )
            connection.commit()
        finally:
            connection.close()

        handler_class = self.mod.build_web_handler(
            db_path=self.db_path,
            static_dir=self.mod.WEB_STATIC_DIR,
            allowed_source_ids=set(),
            restrict_to_source_ids=False,
        )
        server = self.mod.ThreadingHTTPServer(("127.0.0.1", 0), handler_class)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.addCleanup(lambda: thread.join(timeout=3))

        host, port = server.server_address
        first_page_url = f"http://{host}:{port}/api/feed?limit=1&offset=0"
        second_page_url = f"http://{host}:{port}/api/feed?limit=1&offset=1"

        with urllib.request.urlopen(first_page_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            first_page = json.loads(response.read().decode("utf-8"))
        with urllib.request.urlopen(second_page_url, timeout=5) as response:
            self.assertEqual(response.status, 200)
            second_page = json.loads(response.read().decode("utf-8"))

        self.assertEqual([video["video_id"] for video in first_page.get("videos", [])], ["video-valid"])
        self.assertEqual(second_page.get("videos"), [])

    def test_feed_tracks_expose_upstream_vs_generated_origins(self):
        now_iso = dt.datetime(2026, 3, 10, 0, 0, tzinfo=dt.timezone.utc).isoformat()
        media_path = self.workspace_root / "video-origin.mp4"
        media_path.write_bytes(b"")
        upstream_subtitle_path = self.workspace_root / "video-origin.eng-US.vtt"
        upstream_subtitle_path.write_text("WEBVTT\n\n", encoding="utf-8")
        generated_subtitle_path = self.workspace_root / "video-origin.ja.vtt"
        generated_subtitle_path.write_text("WEBVTT\n\n", encoding="utf-8")

        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            connection.execute(
                """
                INSERT INTO videos(
                    source_id,
                    video_id,
                    title,
                    media_path,
                    has_media,
                    synced_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("storiesofcz", "video-origin", "origin", str(media_path), 1, now_iso),
            )
            connection.execute(
                """
                INSERT INTO subtitles(source_id, video_id, language, subtitle_path, ext)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("storiesofcz", "video-origin", "eng-US", str(upstream_subtitle_path), "vtt"),
            )
            connection.execute(
                """
                INSERT INTO subtitles(source_id, video_id, language, subtitle_path, ext)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("storiesofcz", "video-origin", "ja", str(generated_subtitle_path), "vtt"),
            )
            self.mod.record_translation_run(
                connection=connection,
                source_id="storiesofcz",
                video_id="video-origin",
                source_path=upstream_subtitle_path,
                output_path=generated_subtitle_path,
                cue_count=2,
                cue_match=True,
                agent="claude-opus-4-6",
                method="manual-import",
                method_version="manual-import-v1",
                summary="generated ja subtitle",
                source_lang="en",
                target_lang="ja",
                status="active",
                started_at=now_iso,
                finished_at=now_iso,
            )
            self.mod.ensure_subtitles_origin_columns(connection)
            connection.commit()
        finally:
            connection.close()

        handler_class = self.mod.build_web_handler(
            db_path=self.db_path,
            static_dir=self.mod.WEB_STATIC_DIR,
            allowed_source_ids=set(),
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

        video = next(
            item for item in payload.get("videos", [])
            if str(item.get("video_id") or "") == "video-origin"
        )
        tracks = {str(track.get("language") or track.get("label") or ""): track for track in video.get("tracks", [])}
        self.assertEqual(tracks["eng-US"]["origin_kind"], "upstream")
        self.assertEqual(tracks["eng-US"]["origin_label"], "Upstream")
        self.assertIn("[Upstream]", str(tracks["eng-US"]["display_label"]))
        self.assertEqual(tracks["ja"]["origin_kind"], "generated")
        self.assertEqual(tracks["ja"]["origin_label"], "Generated")
        self.assertIn("[Generated]", str(tracks["ja"]["display_label"]))


if __name__ == "__main__":
    unittest.main()
