import importlib.util
import sys
import unittest
from pathlib import Path


def load_substudy_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "substudy.py"
    spec = importlib.util.spec_from_file_location("substudy_network_test", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load scripts/substudy.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class NetworkProfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_substudy_module()

    def test_decide_network_profile_manual_weak(self):
        decision = self.mod.decide_network_profile(
            profile_mode="weak",
            probe_url="https://example.com",
            timeout_sec=3,
            probe_bytes=4096,
            weak_net_min_kbps=800.0,
            weak_net_max_rtt_ms=800.0,
        )
        self.assertEqual(decision.profile, "weak")
        self.assertIn("manual", decision.reason)

    def test_decide_network_profile_auto_weak_by_rtt(self):
        decision = self.mod.decide_network_profile(
            profile_mode="auto",
            probe_url="https://example.com",
            timeout_sec=3,
            probe_bytes=4096,
            weak_net_min_kbps=800.0,
            weak_net_max_rtt_ms=500.0,
            probe_func=lambda *_: (700.0, 1500.0, 4096),
        )
        self.assertEqual(decision.profile, "weak")
        self.assertIn("rtt_ms", decision.reason)

    def test_decide_network_profile_auto_weak_by_kbps(self):
        decision = self.mod.decide_network_profile(
            profile_mode="auto",
            probe_url="https://example.com",
            timeout_sec=3,
            probe_bytes=4096,
            weak_net_min_kbps=1000.0,
            weak_net_max_rtt_ms=800.0,
            probe_func=lambda *_: (100.0, 300.0, 4096),
        )
        self.assertEqual(decision.profile, "weak")
        self.assertIn("kbps", decision.reason)

    def test_decide_network_profile_auto_normal(self):
        decision = self.mod.decide_network_profile(
            profile_mode="auto",
            probe_url="https://example.com",
            timeout_sec=3,
            probe_bytes=4096,
            weak_net_min_kbps=500.0,
            weak_net_max_rtt_ms=700.0,
            probe_func=lambda *_: (120.0, 2200.0, 4096),
        )
        self.assertEqual(decision.profile, "normal")
        self.assertIn("auto probe", decision.reason)

    def test_decide_network_profile_auto_probe_failure_fallback_weak(self):
        def _raise(*_args):
            raise RuntimeError("network down")

        decision = self.mod.decide_network_profile(
            profile_mode="auto",
            probe_url="https://example.com",
            timeout_sec=3,
            probe_bytes=4096,
            weak_net_min_kbps=500.0,
            weak_net_max_rtt_ms=700.0,
            probe_func=_raise,
        )
        self.assertEqual(decision.profile, "weak")
        self.assertIn("probe failed", decision.reason)


if __name__ == "__main__":
    unittest.main()
