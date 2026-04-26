"""P1 校验器单元测试 —— 用 2026-04-26 的 B193/B217/B221 三单做样例。

期望复现的核心结论：
1. parquet datetime 是 UTC+8（不再做时区偏移）
2. B217 high 41.252 始终在 ema100 41.255 之下 → state == 'below_only'
3. B221 旧参数 entry=41.215 不在 OHLC + 后续不可达 + stop 41.285 高于 ema_200
4. B221 校正参数 entry=41.178 entry=close → A PASS, 后验止盈兑现于 B225

运行：python tests/validators/test_validators.py
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
    parse_ref,
    ema_relation,
    validate_trade,
)
from tools.validators.trade_params import format_report  # noqa: E402


class ValidatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.df = load_day("2026-04-26", "HYPE", "5m")

    # ---------- data_loader ----------
    def test_01_datetime_is_utc8_not_utc(self):
        """B116 (09:40 UTC+8) close ≈ 41.359"""
        bar = get_bar(self.df, "B116")
        self.assertEqual(bar["datetime"].strftime("%H:%M"), "09:40")
        self.assertAlmostEqual(bar["close"], 41.359, delta=0.005)

    def test_02_parse_ref_supports_id_and_time(self):
        self.assertEqual(parse_ref(self.df, "B193"), parse_ref(self.df, "16:05"))
        self.assertEqual(parse_ref(self.df, "B7"), parse_ref(self.df, "B07"))

    def test_03_b193_known_values(self):
        bar = get_bar(self.df, "B193")
        self.assertEqual(bar["datetime"].strftime("%H:%M"), "16:05")
        self.assertAlmostEqual(bar["close"], 41.239, delta=0.005)
        self.assertEqual(bar["ema_stack"], "bear_stack")

    # ---------- price_ema ----------
    def test_10_b217_close_below_ema100(self):
        """B217 high 41.252 < ema100 41.255 → ema_100/ema_200 都是 below_only"""
        bar = get_bar(self.df, "B217")
        rel = ema_relation(bar)
        print("\n[B217 ema_relation]\n  " + rel["narrative"])
        self.assertEqual(rel["relation"]["ema_100"]["state"], "below_only")
        self.assertEqual(rel["relation"]["ema_200"]["state"], "below_only")
        self.assertNotIn("ema_100", rel["touched"])
        self.assertNotIn("ema_200", rel["touched"])
        self.assertEqual(rel["rejected_above"], [])
        self.assertEqual(rel["closest_above_close"][0], "ema_100")

    def test_11_b193_touches_ema100(self):
        """B193 high 41.319 vs ema100 41.308 / ema200 41.317 → 应触及"""
        bar = get_bar(self.df, "B193")
        rel = ema_relation(bar)
        print("\n[B193 ema_relation]\n  " + rel["narrative"])
        states = {k: v["state"] for k, v in rel["relation"].items()}
        self.assertIn(states.get("ema_100"), ("rejected_above", "touched_neutral"))

    # ---------- trade_params ----------
    def test_20_b193_original_trade(self):
        r = validate_trade(self.df, "B193", "short", 41.242, 41.336, 41.112, claimed_rr=1.38)
        print("\n" + format_report(r))
        self.assertEqual(r["checks"]["D_rr_math"]["status"], "PASS")
        self.assertEqual(r["checks"]["E_outcome"]["outcome"], "TAKE")

    def test_21_b221_old_params_fail(self):
        """旧参数：entry 41.215 不在 OHLC、后续不可达、stop 高于 ema_200"""
        r = validate_trade(self.df, "B221", "short", 41.215, 41.285, 41.112, claimed_rr=1.47)
        print("\n" + format_report(r))
        self.assertIn(r["checks"]["A_entry_in_ohlc"]["status"], ("WARN", "FAIL"))
        self.assertEqual(r["checks"]["A_entry_in_ohlc"]["matched"], [])
        self.assertEqual(r["checks"]["B_reachability"]["status"], "FAIL")
        # 新逻辑：stop 跨越 ema_200（entry 在 ema_200 下方、stop 在上方）
        self.assertIn("ema_200", r["checks"]["C_stop_position"]["stop_crossed_emas"])
        self.assertIn(r["overall"], ("FAIL", "WARN"))

    def test_22_b221_corrected_plan_a(self):
        """校正参数 A：entry=close 41.178 / stop=41.220 / take=41.123"""
        r = validate_trade(self.df, "B221", "short", 41.178, 41.220, 41.123, claimed_rr=1.31)
        print("\n" + format_report(r))
        self.assertEqual(r["checks"]["A_entry_in_ohlc"]["status"], "PASS")
        self.assertIn("close", r["checks"]["A_entry_in_ohlc"]["matched"])
        self.assertNotIn("ema_200", r["checks"]["C_stop_position"]["stop_crossed_emas"])
        self.assertEqual(r["checks"]["D_rr_math"]["status"], "PASS")
        self.assertEqual(r["checks"]["E_outcome"]["outcome"], "TAKE")
        # 校正预期：B224 low 41.105 是首个 ≤ take 41.123 的根（不是 B225 41.116）
        self.assertEqual(r["checks"]["E_outcome"]["fired_at"], "B224")

    def test_23_b221_plan_b_take_unreached(self):
        """方案 B：止盈 41.097 后续未触及（最低 41.103）"""
        r = validate_trade(self.df, "B221", "short", 41.178, 41.240, 41.097, claimed_rr=1.31)
        print("\n" + format_report(r))
        self.assertNotEqual(r["checks"]["E_outcome"]["outcome"], "TAKE")


if __name__ == "__main__":
    unittest.main(verbosity=2)
