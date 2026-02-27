import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


def load_substudy_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "substudy.py"
    spec = importlib.util.spec_from_file_location("substudy_translate_local_test", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load scripts/substudy.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class TranslateLocalPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_substudy_module()

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.db_path = self.root / "master_ledger.sqlite"
        connection = sqlite3.connect(str(self.db_path))
        try:
            self.mod.create_schema(connection)
            connection.commit()
        finally:
            connection.close()

    def _insert_source_and_video(self, connection, source_id: str, video_id: str) -> None:
        now_iso = "2026-02-23T00:00:00Z"
        connection.execute(
            """
            INSERT OR REPLACE INTO sources (source_id, platform, url, data_dir, updated_at)
            VALUES (?, 'tiktok', ?, ?, ?)
            """,
            (source_id, f"https://example.com/@{source_id}", str(self.root), now_iso),
        )
        connection.execute(
            """
            INSERT OR REPLACE INTO videos (source_id, video_id, has_media, synced_at)
            VALUES (?, ?, 1, ?)
            """,
            (source_id, video_id, now_iso),
        )

    def test_record_translation_run_supersedes_previous_active(self):
        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        try:
            run_id_1 = self.mod.record_translation_run(
                connection=connection,
                source_id="storiesofcz",
                video_id="vid001",
                source_path=Path("/tmp/vid001.en.vtt"),
                output_path=Path("/tmp/vid001.ja.vtt"),
                cue_count=5,
                cue_match=True,
                agent="claude-code",
                method="direct",
                method_version="claude-v1",
                summary="first",
                target_lang="ja",
            )
            run_id_2 = self.mod.record_translation_run(
                connection=connection,
                source_id="storiesofcz",
                video_id="vid001",
                source_path=Path("/tmp/vid001.en.vtt"),
                output_path=Path("/tmp/vid001.ja.vtt"),
                cue_count=5,
                cue_match=True,
                agent="local-llm",
                method="multi-stage",
                method_version="20b+120b",
                summary="second",
                target_lang="ja",
            )
            connection.commit()

            self.assertGreater(run_id_2, run_id_1)
            rows = connection.execute(
                """
                SELECT run_id, status, agent, method
                FROM translation_runs
                WHERE source_id = 'storiesofcz'
                  AND video_id = 'vid001'
                  AND target_lang = 'ja'
                ORDER BY run_id ASC
                """
            ).fetchall()
        finally:
            connection.close()

        self.assertEqual(len(rows), 2)
        self.assertEqual(str(rows[0]["status"]), "superseded")
        self.assertEqual(str(rows[0]["agent"]), "claude-code")
        self.assertEqual(str(rows[1]["status"]), "active")
        self.assertEqual(str(rows[1]["agent"]), "local-llm")
        self.assertEqual(str(rows[1]["method"]), "multi-stage")

    def test_parse_and_render_preserves_cue_timing(self):
        source_path = self.root / "sample.en.vtt"
        output_path = self.root / "sample.ja.vtt"
        source_path.write_text(
            "\n".join(
                [
                    "WEBVTT",
                    "",
                    "00:00:00.000 --> 00:00:01.000",
                    "Hello there.",
                    "",
                    "2",
                    "00:00:01.000 --> 00:00:02.500 align:start",
                    "How are you?",
                    "",
                    "00:00:02.500 --> 00:00:03.500",
                    "I am fine.",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        document = self.mod.parse_subtitle_document(source_path)
        self.assertEqual(len(document.cues), 3)

        translated = {
            1: "こんにちは。",
            2: "元気ですか？",
            3: "私は元気です。",
        }
        rendered = self.mod.render_subtitle_document(document, translated)
        output_path.write_text(rendered, encoding="utf-8")

        source_cues = self.mod.parse_subtitle_cues(source_path)
        output_cues = self.mod.parse_subtitle_cues(output_path)
        self.assertEqual(len(source_cues), len(output_cues))
        for source_cue, output_cue in zip(source_cues, output_cues):
            self.assertEqual(source_cue["start_ms"], output_cue["start_ms"])
            self.assertEqual(source_cue["end_ms"], output_cue["end_ms"])
        self.assertIn("00:00:01.000 --> 00:00:02.500 align:start", rendered)

    def test_collect_local_translation_targets_supports_subtitle_asr_and_auto(self):
        source_id = "careervidz"
        video_id = "7299838852636757281"
        subtitle_path = self.root / f"{video_id}.NA.eng-US.vtt"
        asr_path = self.root / f"{video_id}.asr.vtt"
        subtitle_path.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nhello\n", encoding="utf-8")
        asr_path.write_text("WEBVTT\n\n00:00:00.000 --> 00:00:01.000\nhello from asr\n", encoding="utf-8")

        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        try:
            self._insert_source_and_video(connection, source_id, video_id)
            connection.execute(
                """
                INSERT INTO subtitles (source_id, video_id, language, subtitle_path, ext)
                VALUES (?, ?, ?, ?, ?)
                """,
                (source_id, video_id, "NA.eng-US", str(subtitle_path), "vtt"),
            )
            connection.execute(
                """
                INSERT INTO asr_runs (
                    source_id, video_id, status, output_path, artifact_dir,
                    engine, attempts, last_error, started_at, finished_at, updated_at
                ) VALUES (?, ?, 'success', ?, ?, 'command', 1, NULL, ?, ?, ?)
                """,
                (
                    source_id,
                    video_id,
                    str(asr_path),
                    str(self.root / "asr-artifact"),
                    "2026-02-23T00:00:00Z",
                    "2026-02-23T00:01:00Z",
                    "2026-02-23T00:01:00Z",
                ),
            )
            connection.commit()

            subtitle_targets = self.mod.collect_local_translation_targets(
                connection=connection,
                source_ids=[source_id],
                target_lang="ja-local",
                source_track="subtitle",
                video_ids=None,
                limit=10,
                include_translated=False,
                overwrite=False,
            )
            asr_targets = self.mod.collect_local_translation_targets(
                connection=connection,
                source_ids=[source_id],
                target_lang="ja-asr-local",
                source_track="asr",
                video_ids=None,
                limit=10,
                include_translated=False,
                overwrite=False,
            )
            auto_targets = self.mod.collect_local_translation_targets(
                connection=connection,
                source_ids=[source_id],
                target_lang="ja-local",
                source_track="auto",
                video_ids=None,
                limit=10,
                include_translated=False,
                overwrite=False,
            )
        finally:
            connection.close()

        self.assertEqual(len(subtitle_targets), 1)
        self.assertEqual(subtitle_targets[0]["source_track_kind"], "subtitle")
        self.assertEqual(Path(subtitle_targets[0]["subtitle_path"]), subtitle_path)
        self.assertEqual(Path(subtitle_targets[0]["output_path"]).name, f"{video_id}.ja-local.vtt")

        self.assertEqual(len(asr_targets), 1)
        self.assertEqual(asr_targets[0]["source_track_kind"], "asr")
        self.assertEqual(Path(asr_targets[0]["subtitle_path"]), asr_path)
        self.assertEqual(Path(asr_targets[0]["output_path"]).name, f"{video_id}.ja-asr-local.vtt")

        self.assertEqual(len(auto_targets), 1)
        self.assertEqual(auto_targets[0]["source_track_kind"], "subtitle")
        self.assertEqual(Path(auto_targets[0]["subtitle_path"]), subtitle_path)

    def test_collect_local_translation_targets_auto_falls_back_to_asr_when_no_subtitle(self):
        source_id = "careervidz"
        video_id = "7309803358792060192"
        asr_path = self.root / f"{video_id}.asr.srt"
        asr_path.write_text(
            "\n".join(
                [
                    "1",
                    "00:00:00,000 --> 00:00:01,000",
                    "fallback asr cue",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        connection = sqlite3.connect(str(self.db_path))
        connection.row_factory = sqlite3.Row
        try:
            self._insert_source_and_video(connection, source_id, video_id)
            connection.execute(
                """
                INSERT INTO asr_runs (
                    source_id, video_id, status, output_path, artifact_dir,
                    engine, attempts, last_error, started_at, finished_at, updated_at
                ) VALUES (?, ?, 'success', ?, ?, 'command', 1, NULL, ?, ?, ?)
                """,
                (
                    source_id,
                    video_id,
                    str(asr_path),
                    str(self.root / "asr-artifact"),
                    "2026-02-23T00:00:00Z",
                    "2026-02-23T00:01:00Z",
                    "2026-02-23T00:01:00Z",
                ),
            )
            connection.commit()

            targets = self.mod.collect_local_translation_targets(
                connection=connection,
                source_ids=[source_id],
                target_lang="ja-asr-local",
                source_track="auto",
                video_ids=None,
                limit=10,
                include_translated=False,
                overwrite=False,
            )
        finally:
            connection.close()

        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0]["source_track_kind"], "asr")
        self.assertEqual(Path(targets[0]["subtitle_path"]), asr_path)
        self.assertEqual(Path(targets[0]["output_path"]).name, f"{video_id}.ja-asr-local.srt")

    def test_evaluate_translation_quality_detects_core_issues(self):
        source_path = self.root / "quality.en.vtt"
        source_path.write_text(
            "\n".join(
                [
                    "WEBVTT",
                    "",
                    "00:00:00.000 --> 00:00:01.000",
                    "Hello there.",
                    "",
                    "00:00:01.000 --> 00:00:02.000",
                    "This line remains in English.",
                    "",
                    "00:00:02.000 --> 00:00:03.000",
                    "Done.",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        document = self.mod.parse_subtitle_document(source_path)
        source_texts = self.mod.build_source_text_by_cue_id(document)
        translations = {
            1: '{"ja":"',
            2: "This line remains in English.",
            3: "完了。",
        }

        report = self.mod.evaluate_translation_quality(
            document=document,
            translations=translations,
            source_text_by_cue_id=source_texts,
        )

        self.assertEqual(report.total_cues, 3)
        self.assertEqual(report.json_fragment_cues, 1)
        self.assertEqual(report.unchanged_cues, 1)
        self.assertGreaterEqual(report.english_heavy_cues, 1)
        self.assertEqual(report.bad_cue_ids, [1, 2])

    def test_quality_report_threshold_check(self):
        report = self.mod.TranslationQualityReport(
            total_cues=10,
            json_fragment_cues=0,
            english_heavy_cues=1,
            unchanged_cues=1,
            empty_cues=0,
            bad_cue_ids=[3, 8],
        )
        self.assertTrue(
            self.mod.quality_report_passes_thresholds(
                report=report,
                json_fragment_threshold=0.0,
                english_heavy_threshold=0.20,
                unchanged_threshold=0.20,
            )
        )
        self.assertFalse(
            self.mod.quality_report_passes_thresholds(
                report=report,
                json_fragment_threshold=0.0,
                english_heavy_threshold=0.05,
                unchanged_threshold=0.20,
            )
        )

    def test_extract_audit_issue_map_from_llm_output(self):
        raw = (
            '{"cues":['
            '{"cue_id":1,"issue":"json_fragment","needs_fix":true},'
            '{"cue_id":2,"issue":"","needs_fix":false},'
            '{"cue_id":"3","issue":"","needs_fix":"yes"}'
            ']}'
        )
        issue_map = self.mod.extract_audit_issue_map_from_llm_output(raw)
        self.assertEqual(issue_map.get(1), "json_fragment")
        self.assertNotIn(2, issue_map)
        self.assertEqual(issue_map.get(3), "check")


if __name__ == "__main__":
    unittest.main()
