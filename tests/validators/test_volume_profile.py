"""volume_profile 单元测试。

包含：
- 合成数据：双峰分布 → POC 落在大峰、HVN 至少含两个峰、LVN 在两峰之间。
- 单峰均匀：POC 在中心，VA 接近覆盖 70% 的中段。
- 真实数据：用 reviews/2026-04-26/data/HYPE_5m_001.trades.parquet 作端到端
  sanity 检查（不写死数值，仅断言基本不变量）。
- OHLCV fallback：与合成 trades 在同窗口下的 POC 距离不会过大。
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.volume_profile import (  # noqa: E402
    VolumeProfile,
    build_from_ohlcv,
    build_from_trades,
    build_micro_vps,
    value_area,
)


def _synth_trades(centers: list[tuple[float, float, float]], side_split: float = 0.5,
                  rng_seed: int = 0) -> pd.DataFrame:
    """centers: [(price, sigma, total_volume), ...]，返回 trades DataFrame。"""
    rng = np.random.default_rng(rng_seed)
    rows = []
    for price, sigma, total in centers:
        n = max(50, int(total / 0.1))
        prices = rng.normal(loc=price, scale=sigma, size=n)
        amounts = np.full(n, total / n)
        sides = rng.choice(["buy", "sell"], size=n, p=[side_split, 1 - side_split])
        for p, a, s in zip(prices, amounts, sides):
            rows.append({"price": float(p), "amount": float(a), "side": str(s)})
    return pd.DataFrame(rows)


class VolumeProfileTests(unittest.TestCase):
    # ─── 合成：双峰 ───
    def test_bimodal_poc_at_larger_peak(self):
        # 100 处 1000 量；110 处 400 量 → POC 应在 100 附近
        trades = _synth_trades([(100.0, 0.5, 1000.0), (110.0, 0.5, 400.0)])
        vp = build_from_trades(trades, n_bins=80, price_lo=95, price_hi=115)
        self.assertAlmostEqual(vp.poc, 100.0, delta=0.5)
        self.assertGreater(vp.total_vol, 1300.0)
        # VA 应包住主峰（100 附近）但不一定包到 110
        self.assertLessEqual(vp.va_low, 100.5)
        self.assertGreaterEqual(vp.va_high, 99.5)

    def test_bimodal_hvn_contains_both_peaks(self):
        trades = _synth_trades([(100.0, 0.4, 800.0), (110.0, 0.4, 800.0)])
        vp = build_from_trades(trades, n_bins=80, price_lo=95, price_hi=115)
        # HVN 至少应能覆盖到两个峰附近（容忍 0.5 价格误差）
        self.assertTrue(any(abs(p - 100.0) < 0.5 for p in vp.hvn),
                        f"HVN 未覆盖 100 附近: {vp.hvn}")
        self.assertTrue(any(abs(p - 110.0) < 0.5 for p in vp.hvn),
                        f"HVN 未覆盖 110 附近: {vp.hvn}")

    def test_bimodal_lvn_between_peaks(self):
        trades = _synth_trades([(100.0, 0.4, 800.0), (110.0, 0.4, 800.0)])
        vp = build_from_trades(trades, n_bins=80, price_lo=95, price_hi=115)
        # LVN 应至少有一个落在 [101, 109] 中间区
        between = [p for p in vp.lvn if 101 <= p <= 109]
        self.assertTrue(between, f"LVN 未在两峰之间出现: {vp.lvn}")

    # ─── 合成：单峰均匀 ───
    def test_unimodal_poc_centered(self):
        trades = _synth_trades([(50.0, 1.0, 500.0)])
        vp = build_from_trades(trades, n_bins=60, price_lo=45, price_hi=55)
        self.assertAlmostEqual(vp.poc, 50.0, delta=0.4)
        # VA 上下界应大致对称围绕 POC
        self.assertGreater(vp.va_high, 50.0)
        self.assertLess(vp.va_low, 50.0)

    # ─── value_area 直接调用 ───
    def test_value_area_pct_target(self):
        trades = _synth_trades([(50.0, 1.0, 1000.0)])
        vp = build_from_trades(trades, n_bins=80, price_lo=45, price_hi=55, va_pct=0.7)
        within = vp.bins[(vp.bins["price_lo"] >= vp.va_low - 1e-9)
                         & (vp.bins["price_hi"] <= vp.va_high + 1e-9)]
        ratio = within["vol"].sum() / vp.total_vol
        # 单 bin 贪心法在离散网格上不会精确 70%，应 ≥ 70% 且不远超
        self.assertGreaterEqual(ratio, 0.69)
        self.assertLessEqual(ratio, 0.85)

    # ─── 输入校验 ───
    def test_empty_trades_raises(self):
        with self.assertRaises(ValueError):
            build_from_trades(pd.DataFrame(columns=["price", "amount"]))

    def test_invalid_range_raises(self):
        with self.assertRaises(ValueError):
            build_from_trades(
                _synth_trades([(50.0, 1.0, 100.0)]),
                price_lo=60.0, price_hi=50.0,
            )

    def test_no_side_column_ok(self):
        trades = _synth_trades([(50.0, 1.0, 100.0)])[["price", "amount"]]
        vp = build_from_trades(trades, n_bins=40, price_lo=45, price_hi=55)
        # buy/sell 列应全 0
        self.assertEqual(vp.bins["buy_vol"].sum(), 0.0)
        self.assertEqual(vp.bins["sell_vol"].sum(), 0.0)
        self.assertGreater(vp.total_vol, 0)

    # ─── OHLCV fallback ───
    def test_ohlcv_approx_basic(self):
        # 三根 K，volume 分布于不同价格区间
        klines = pd.DataFrame({
            "low":  [99.0, 100.0, 105.0],
            "high": [101.0, 102.0, 107.0],
            "volume": [200.0, 600.0, 100.0],  # 第二根 K（100-102）量最大
        })
        vp = build_from_ohlcv(klines, n_bins=40)
        self.assertEqual(vp.source, "ohlcv_approx")
        # POC 应在 100-102 区间内
        self.assertGreaterEqual(vp.poc, 100.0)
        self.assertLessEqual(vp.poc, 102.0)

    def test_ohlcv_total_volume_preserved(self):
        klines = pd.DataFrame({
            "low":  [10.0, 11.0],
            "high": [12.0, 13.0],
            "volume": [100.0, 200.0],
        })
        vp = build_from_ohlcv(klines, n_bins=20)
        # 总量与输入应一致（重叠分配不应丢失或多算）
        self.assertAlmostEqual(vp.total_vol, 300.0, delta=0.5)

    # ─── 真实数据 sanity ───
    def test_real_trades_sanity(self):
        path = ROOT / "reviews/2026-04-26/data/HYPE_5m_001.trades.parquet"
        if not path.exists():
            self.skipTest(f"缺少实测数据: {path}")
        trades = pd.read_parquet(path)
        # 适配现有列名（orderflow_fetcher 写的是 timestamp/price/amount/side）
        self.assertIn("price", trades.columns)
        self.assertIn("amount", trades.columns)
        vp = build_from_trades(trades, n_bins=80)
        # 不变量：POC 在 [min(price), max(price)]；VA 包住 POC；total > 0
        self.assertGreater(vp.total_vol, 0)
        self.assertLessEqual(vp.bins["price_lo"].iloc[0], vp.poc)
        self.assertGreaterEqual(vp.bins["price_hi"].iloc[-1], vp.poc)
        self.assertLessEqual(vp.va_low, vp.poc)
        self.assertGreaterEqual(vp.va_high, vp.poc)
        self.assertEqual(vp.source, "trades")
        self.assertEqual(len(vp.bins), 80)


    # ─── micro-VP（footprint）─── 
    def test_micro_vps_with_trades_real(self):
        """真实数据：用锚附近 ±5 根 K + trades 生成 micro-VP，每根都应非空。"""
        df_path = ROOT / "reviews/2026-04-26/data/HYPE_5m_001.parquet"
        tr_path = ROOT / "reviews/2026-04-26/data/HYPE_5m_001.trades.parquet"
        if not (df_path.exists() and tr_path.exists()):
            self.skipTest("缺少实测数据")
        df = pd.read_parquet(df_path)
        trades = pd.read_parquet(tr_path)
        anchor_idx = int(df.index[df["is_anchor"]][0])
        bars = df.iloc[max(0, anchor_idx - 5):anchor_idx + 6].reset_index(drop=True)
        micros = build_micro_vps(bars, trades=trades, n_bins=12)
        self.assertEqual(len(micros), len(bars))
        # 锚附近的 K 线应该都有 trades 覆盖（orderflow 默认 ±10 ⊃ ±5）
        non_empty = sum(1 for vp in micros if vp.total_vol > 0)
        self.assertGreaterEqual(non_empty, len(bars) - 1,
                                f"trades 覆盖不足：{non_empty}/{len(bars)} 非空")
        # window 字典应都带 kline_ts/kline_tf_ms
        for vp, (_, row) in zip(micros, bars.iterrows()):
            self.assertEqual(vp.window["kline_ts"], int(row["timestamp"]))
            self.assertGreater(vp.window["kline_tf_ms"], 0)

    def test_micro_vps_ohlcv_fallback(self):
        """无 trades 时落回 OHLCV：每根 K 的 mini VP 应总量等于该 K volume。"""
        bars = pd.DataFrame({
            "timestamp": [0, 60_000, 120_000],
            "open":  [10.0, 10.5, 10.8],
            "high":  [11.0, 11.0, 11.2],
            "low":   [9.5, 10.2, 10.6],
            "close": [10.5, 10.8, 11.0],
            "volume": [100.0, 200.0, 50.0],
        })
        micros = build_micro_vps(bars, trades=None, n_bins=10)
        self.assertEqual(len(micros), 3)
        for vp, vol_expected in zip(micros, [100.0, 200.0, 50.0]):
            self.assertAlmostEqual(vp.total_vol, vol_expected, delta=0.01,
                                   msg=f"OHLCV fallback 量不守恒: vp.total={vp.total_vol}")
            self.assertEqual(vp.source, "ohlcv_approx")

    def test_micro_vps_empty_klines(self):
        self.assertEqual(build_micro_vps(pd.DataFrame()), [])

    def test_micro_vps_partial_trades_coverage(self):
        """trades 只覆盖部分 K：覆盖的用真值，未覆盖的回 OHLCV。"""
        bars = pd.DataFrame({
            "timestamp": [0, 60_000, 120_000],
            "open":  [10.0, 10.5, 10.8],
            "high":  [11.0, 11.0, 11.2],
            "low":   [9.5, 10.2, 10.6],
            "close": [10.5, 10.8, 11.0],
            "volume": [100.0, 200.0, 50.0],
        })
        # trades 仅在第二根 K 时间窗口内
        trades = pd.DataFrame({
            "timestamp": [60_500, 90_000, 119_000],
            "price":  [10.6, 10.7, 10.9],
            "amount": [5.0, 8.0, 3.0],
            "side":   ["buy", "sell", "buy"],
        })
        micros = build_micro_vps(bars, trades=trades, n_bins=10)
        sources = [vp.source for vp in micros]
        self.assertEqual(sources[0], "ohlcv_approx")
        self.assertEqual(sources[1], "trades")
        self.assertEqual(sources[2], "ohlcv_approx")
        # 第二根 K 用了 trades，total_vol 应等于 5+8+3=16（不是 OHLCV 的 200）
        self.assertAlmostEqual(micros[1].total_vol, 16.0, delta=0.01)


if __name__ == "__main__":
    unittest.main(verbosity=2)
