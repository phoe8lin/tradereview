"""bar_features + ema_regime 测试（基于 2026-04-26 真实数据）。

期望：
- B193 (upW=0.53, body=0.46, 阴线, delta=+1766) → long_upper_wick + bear_absorption
- B199 (upW=0.97, body=0.03) → long_upper_wick + doji + pin_bar_top
- B204 (loW=0.46, 阳线, delta=-1960, vol=1.78) → long_lower_wick + bull_absorption
- B221 (body=0.95, 阴线, delta=-4229) → 没有 absorption（同向放量），但是 high_volume? 不，vol=1.12 不算
- B225 (engulf='bear') → engulf_bear
- B116 (普通根) → 没有显著形态

regime_segments 2026-04-26 应有 10 段 / 9 次切换；下午 15:10 (B182) 才转 bear_stack；
所以"全天 bear 趋势日" 错误 — 真实主导是 tangled。
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.validators import (  # noqa: E402
    load_day,
    get_bar,
    detect_features,
    scan_features,
    regime_segments,
    regime_at,
    transitions,
    regime_summary,
)
from tools.validators.bar_features import format_feature_line  # noqa: E402
from tools.validators.ema_regime import format_segments  # noqa: E402


class BarFeatureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.df = load_day("2026-04-26", "HYPE", "5m")

    def _detect(self, bar_id):
        return detect_features(get_bar(self.df, bar_id))

    def test_b193_bear_absorption(self):
        """B193 阴线 + delta +1766 → bear_absorption + long_upper_wick"""
        r = self._detect("B193")
        print("\n" + format_feature_line(r))
        self.assertIn("long_upper_wick", r.patterns)
        self.assertIn("bear_absorption", r.patterns)
        self.assertEqual(r.evidence["bear_absorption"]["is_bull"], False)

    def test_b199_pin_bar_top(self):
        """B199 upW=0.97 body=0.03 → pin_bar_top + long_upper_wick + doji"""
        r = self._detect("B199")
        print("\n" + format_feature_line(r))
        self.assertIn("pin_bar_top", r.patterns)
        self.assertIn("long_upper_wick", r.patterns)
        self.assertIn("doji", r.patterns)

    def test_b204_bull_absorption(self):
        """B204 阳线 + delta=-1960 vol=1.78 → bull_absorption + long_lower_wick"""
        r = self._detect("B204")
        print("\n" + format_feature_line(r))
        self.assertIn("long_lower_wick", r.patterns)
        self.assertIn("bull_absorption", r.patterns)
        self.assertEqual(r.evidence["bull_absorption"]["is_bull"], True)

    def test_b221_no_absorption(self):
        """B221 阴线 + delta -4229（同向）→ 不应判定 absorption"""
        r = self._detect("B221")
        print("\n" + format_feature_line(r))
        self.assertNotIn("bull_absorption", r.patterns)
        self.assertNotIn("bear_absorption", r.patterns)
        # body=0.95 不算 doji
        self.assertNotIn("doji", r.patterns)

    def test_b225_engulf_bear(self):
        """B225 parquet engulf='bear' → engulf_bear"""
        r = self._detect("B225")
        print("\n" + format_feature_line(r))
        self.assertIn("engulf_bear", r.patterns)

    def test_b116_no_significant(self):
        """B116 是普通 K 线，不应有强形态（pin/absorption/doji）"""
        r = self._detect("B116")
        print("\n" + format_feature_line(r))
        self.assertNotIn("pin_bar_top", r.patterns)
        self.assertNotIn("pin_bar_bottom", r.patterns)
        self.assertNotIn("bull_absorption", r.patterns)
        self.assertNotIn("bear_absorption", r.patterns)
        self.assertNotIn("doji", r.patterns)

    def test_scan_in_range(self):
        """B190:B225 段内扫 absorption，应至少包含 B193 (bear) 和 B204 (bull)"""
        results = scan_features(self.df, "B190:B225",
                                patterns=["bull_absorption", "bear_absorption"])
        ids = [r.bar_id for r in results]
        print(f"\n[absorption in B190:B225] {ids}")
        self.assertIn("B193", ids)
        self.assertIn("B204", ids)


class EmaRegimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.df = load_day("2026-04-26", "HYPE", "5m")

    def test_regime_segments_count(self):
        segs = regime_segments(self.df)
        print("\n" + format_segments(self.df))
        # 实测 10 段 / 9 切换
        self.assertEqual(len(segs), 10)
        self.assertEqual(segs[0].start_id, "B00")
        self.assertEqual(segs[-1].end_id, "B230")

    def test_bear_stack_starts_late(self):
        """下午 15:10 (B182) 才转 bear_stack —— 全日最后一段"""
        segs = regime_segments(self.df)
        bear_segs = [s for s in segs if s.stack == "bear_stack"]
        # 应有两段 bear_stack：早间 B27-B55 + 下午 B182-B230
        self.assertEqual(len(bear_segs), 2)
        last_bear = bear_segs[-1]
        self.assertEqual(last_bear.start_id, "B182")
        self.assertEqual(last_bear.start_time, "15:10")
        self.assertEqual(last_bear.end_id, "B230")

    def test_b193_in_bear_stack(self):
        """B193 应在 bear_stack 段内"""
        seg = regime_at(self.df, "B193")
        self.assertEqual(seg.stack, "bear_stack")
        self.assertEqual(seg.start_id, "B182")

    def test_b123_in_bull_stack(self):
        """B123 (10:15) 应在 bull_stack 段内（B121-B139）"""
        seg = regime_at(self.df, "B123")
        self.assertEqual(seg.stack, "bull_stack")

    def test_summary_not_trend_day(self):
        """全天主导 stack 是 tangled（约 41%），不是趋势日"""
        s = regime_summary(self.df)
        print(f"\nsummary = {s}")
        self.assertEqual(s["dominant_stack"], "tangled")
        self.assertFalse(s["is_trend_day"])
        self.assertEqual(s["n_transitions"], 9)

    def test_transitions_have_b182(self):
        """切换列表应包含 B182 (tangled→bear_stack)"""
        ts = transitions(self.df)
        b182_t = [t for t in ts if t["at_id"] == "B182"]
        self.assertEqual(len(b182_t), 1)
        self.assertEqual(b182_t[0]["from"], "tangled")
        self.assertEqual(b182_t[0]["to"], "bear_stack")


if __name__ == "__main__":
    unittest.main(verbosity=2)
