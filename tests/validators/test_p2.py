"""P2 校验器测试：signal_score / post_verify / run_review

期望复现：
- 单② B122→B123 应该 5/5 命中（A+ 级）
- 单③ B159→B160 应该约 2/5（C 级）—— delta 仍正、CVD 仍升
- 单① B115→B116 应该约 1/5（D 级）—— vol 没萎缩、delta 仍正、距 ema55 仅 +0.012
- post_verify 对 B221 校正参数：兑现于 B224，max MFE 在 B226（持仓最低点）
- run_review 能成功生成完整 markdown
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.validators import (  # noqa: E402
    load_day,
    score_signal,
    post_verify,
)
from tools.validators.signal_score import format_score_report  # noqa: E402
from tools.validators.post_verify import format_timeline  # noqa: E402
from tools.validators.run_review import render_review  # noqa: E402


class SignalScoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.df = load_day("2026-04-26", "HYPE", "5m")

    def test_single_2_full_marks(self):
        """单② B122→B123：5/5 完美模板"""
        r = score_signal(self.df, "B122", "B123", "short_reversal_overbought")
        print("\n" + format_score_report(r))
        self.assertEqual(r["score"], 5)
        self.assertEqual(r["grade"], "A+")

    def test_single_3_partial(self):
        """单③ B159→B160：delta 仍正 + CVD 仍升 → 应低分"""
        r = score_signal(self.df, "B159", "B160", "short_reversal_overbought")
        print("\n" + format_score_report(r))
        # delta 仍正、cvd 仍上升、vol 没萎缩 ≤ 0.6
        self.assertLess(r["score"], 4)
        # ema55 空间应通过
        names = {it["name"]: it["pass"] for it in r["items"]}
        self.assertTrue(names["distance_to_ema55"])
        self.assertFalse(names["entry_delta_negative"])
        self.assertFalse(names["cvd_top_turned"])

    def test_single_1_low_score(self):
        """单① B115→B116：tangled + 多项不达标"""
        r = score_signal(self.df, "B115", "B116", "short_reversal_overbought")
        print("\n" + format_score_report(r))
        names = {it["name"]: it["pass"] for it in r["items"]}
        # 锚 K 没放量 (0.98)
        self.assertFalse(names["anchor_vol_vs_ma"])
        # 入场 K 没萎缩 (1.16)
        self.assertFalse(names["entry_vol_shrink"])
        # delta 仍正 (+1485)
        self.assertFalse(names["entry_delta_negative"])
        # 距 ema55 仅 +0.012 < 0.06
        self.assertFalse(names["distance_to_ema55"])
        self.assertLessEqual(r["score"], 2)


class PostVerifyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.df = load_day("2026-04-26", "HYPE", "5m")

    def test_b221_plan_a_timeline(self):
        """B221 方案 A：兑现于 B224；最大 MFE 在更后面"""
        r = post_verify(self.df, "B221", "short", 41.178, 41.220, 41.123, horizon=20)
        print("\n" + format_timeline(r, max_rows=10))
        self.assertEqual(r["summary"]["outcome"], "TAKE")
        self.assertEqual(r["summary"]["fired_at"], "B224")
        self.assertEqual(r["summary"]["bars_to_fire"], 3)
        # MAE 应该很小（B221 收盘后没什么反弹）
        self.assertLessEqual(r["summary"]["max_mae_R"], 0.5)

    def test_b193_full_timeline_to_take(self):
        """B193 单：MFE 应远大于 1R（实际 1.6R 左右）"""
        r = post_verify(self.df, "B193", "short", 41.242, 41.336, 41.112, horizon=30)
        print("\n" + format_timeline(r, max_rows=15))
        self.assertEqual(r["summary"]["outcome"], "TAKE")
        self.assertGreater(r["summary"]["max_mfe_R"], 1.3)


class RunReviewIntegrationTest(unittest.TestCase):
    def test_render_review_produces_markdown(self):
        yaml_path = ROOT / "reviews" / "2026-04-26" / "trades_validate.yaml"
        self.assertTrue(yaml_path.exists(), f"缺少样例 yaml: {yaml_path}")
        md = render_review(yaml_path)
        # 基本 sanity
        self.assertIn("自动校验报告", md)
        self.assertIn("B193", md)
        self.assertIn("B221", md)
        self.assertIn("汇总", md)
        # 包含七笔
        for tid in ("B116", "B123", "B160", "B193", "B199", "B217", "B221"):
            self.assertIn(tid, md)
        # 写到磁盘方便人工查看
        out = ROOT / "reviews" / "2026-04-26" / "auto_validation.md"
        out.write_text(md, encoding="utf-8")
        print(f"\n[渲染样例] -> {out}  长度 {len(md)} chars")


if __name__ == "__main__":
    unittest.main(verbosity=2)
