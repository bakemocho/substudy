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


if __name__ == "__main__":
    unittest.main()
