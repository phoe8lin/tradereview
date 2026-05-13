"""build_chart smoke test：用合成 K 线驱动 build_chart，断言生成的 HTML
包含关键结构（4 行布局 axis 名 + VP 概念面板 + Entry/Stop/Take）。

不依赖网络与外部 parquet；纯本地、跑得快。
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.chart_replicator import build_chart  # noqa: E402
from tools.volume_profile import build_from_ohlcv, build_micro_vps  # noqa: E402


def _synth_df(n: int = 60, anchor_idx: int = 30) -> pd.DataFrame:
    """构造满足 build_chart 入参契约的最小 df。"""
    rng = np.random.default_rng(0)
    start = pd.Timestamp("2026-05-01 10:00")
    tf_ms = 5 * 60 * 1000
    ts = [int(start.value // 10**6) + i * tf_ms for i in range(n)]
    dt = pd.to_datetime(ts, unit="ms")

    base = 100 + np.cumsum(rng.normal(0, 0.2, n))
    high = base + rng.uniform(0.1, 0.4, n)
    low = base - rng.uniform(0.1, 0.4, n)
    openp = base + rng.uniform(-0.1, 0.1, n)
    close = base + rng.uniform(-0.1, 0.1, n)
    vol = rng.uniform(800, 1500, n)

    df = pd.DataFrame({
        "timestamp": ts,
        "datetime": dt,
        "open": openp, "high": high, "low": low, "close": close,
        "volume": vol,
        "wave": np.sin(np.arange(n) / 4) * 50,
        "body_ratio": rng.uniform(0.2, 0.8, n),
        "upper_wick_ratio": rng.uniform(0.0, 0.4, n),
        "lower_wick_ratio": rng.uniform(0.0, 0.4, n),
        "vol_vs_ma": rng.uniform(0.8, 1.5, n),
        "engulf": [""] * n,
        "ema_21": base, "ema_55": base - 0.3,
        "ema_100": base - 0.6, "ema_200": base - 1.0,
        "in_display": [True] * n,
        "is_anchor": [i == anchor_idx for i in range(n)],
        "kline_id": [f"A{i - anchor_idx}" if i != anchor_idx else "A0" for i in range(n)],
    })
    return df


class TestBuildChartSmoke(unittest.TestCase):

    def test_minimal_no_vp(self):
        df = _synth_df()
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "chart.html"
            path = build_chart(
                df, title="synth", entry=100.0, stop=99.0, take=102.0,
                direction="long", output_html=str(out),
            )
            self.assertTrue(Path(path).exists())
            txt = Path(path).read_text(encoding="utf-8")
            # 主图 + Wave 至少两行
            self.assertIn("xaxis", txt)
            self.assertIn("Wave Filter", txt)
            # 入场/止损/止盈线 & 信息卡
            self.assertIn("Entry", txt)
            self.assertIn("Stop", txt)
            self.assertIn("Take", txt)
            self.assertIn("RR=", txt)
            # 无 VP 时不应注入概念面板
            self.assertNotIn("vp-concept-panel", txt)

    def test_with_vp_and_micro(self):
        df = _synth_df()
        anchor_idx = int(df.index[df["is_anchor"]][0])
        big_vp = build_from_ohlcv(df, n_bins=40, va_pct=0.70)
        bars = df.iloc[max(0, anchor_idx - 5):anchor_idx + 6].reset_index(drop=True)
        micro_vps = build_micro_vps(bars, trades=None, n_bins=10)
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "chart.html"
            path = build_chart(
                df, title="synth-vp",
                entry=100.0, stop=99.0, take=102.0,
                direction="long", output_html=str(out),
                big_vp=big_vp, micro_vps=micro_vps,
            )
            txt = Path(path).read_text(encoding="utf-8")
            # 4 行布局：wave + VP 必然引入更多 yaxis
            self.assertIn("yaxis3", txt)
            # VP 概念速查面板被注入（可折叠 details）
            self.assertIn("vp-concept-panel", txt)
            self.assertIn("VP 概念速查", txt)
            # VP hover 提示标识点（散点 trace 用 _vp_tip 命名 + POC 说明）
            self.assertIn("Point of Control", txt)
            # VP 子图标题
            self.assertIn("Volume Profile", txt)

    def test_panel_injection_idempotent(self):
        """重复对同一 HTML 调用注入不会重复堆叠样式。"""
        from tools.chart_replicator import _inject_concept_panel
        df = _synth_df()
        big_vp = build_from_ohlcv(df, n_bins=20, va_pct=0.70)
        with tempfile.TemporaryDirectory() as td:
            out = Path(td) / "chart.html"
            build_chart(
                df, title="x", output_html=str(out),
                big_vp=big_vp,
            )
            before = Path(out).read_text(encoding="utf-8")
            _inject_concept_panel(str(out))
            after = Path(out).read_text(encoding="utf-8")
            self.assertEqual(before, after)
            self.assertEqual(after.count("vp-concept-panel"), before.count("vp-concept-panel"))


if __name__ == "__main__":
    unittest.main()
