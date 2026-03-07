import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


def load_substudy_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "substudy.py"
    spec = importlib.util.spec_from_file_location("substudy_loudness_test", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load scripts/substudy.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class LoudnessAnalysisTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_substudy_module()

    def test_analyze_media_loudness_reports_silent_audio_for_negative_inf(self):
        completed = SimpleNamespace(
            returncode=0,
            stderr='{"input_i":"-inf","input_tp":"-inf"}',
            stdout="",
        )
        with mock.patch.object(self.mod.subprocess, "run", return_value=completed):
            input_lufs, error = self.mod.analyze_media_loudness(
                media_path=Path("/tmp/fake.mp4"),
                ffmpeg_bin="ffmpeg",
                target_lufs=-16.0,
            )
        self.assertIsNone(input_lufs)
        self.assertTrue(self.mod.is_silent_audio_loudness_error(error))

    def test_analyze_media_loudness_returns_finite_lufs(self):
        completed = SimpleNamespace(
            returncode=0,
            stderr='{"input_i":"-20.14","input_tp":"-1.2"}',
            stdout="",
        )
        with mock.patch.object(self.mod.subprocess, "run", return_value=completed):
            input_lufs, error = self.mod.analyze_media_loudness(
                media_path=Path("/tmp/fake.mp4"),
                ffmpeg_bin="ffmpeg",
                target_lufs=-16.0,
            )
        self.assertIsNotNone(input_lufs)
        self.assertAlmostEqual(float(input_lufs), -20.14, places=2)
        self.assertIsNone(error)

    def test_is_negative_infinite_loudnorm_value(self):
        self.assertTrue(self.mod.is_negative_infinite_loudnorm_value("-inf"))
        self.assertTrue(self.mod.is_negative_infinite_loudnorm_value(float("-inf")))
        self.assertFalse(self.mod.is_negative_infinite_loudnorm_value("-20.0"))


if __name__ == "__main__":
    unittest.main()
