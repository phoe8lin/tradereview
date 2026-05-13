"""成交量分布（Volume Profile / VPVR）核心计算模块。

设计要点
--------
1. **窗口由调用方决定**：本模块只负责把"一段 trades 或一段 K 线"换算成
   价格分箱直方图 + POC / VA / HVN / LVN，不在内部硬编码窗口规则。
2. **两条数据路径**：
   - `build_from_trades(trades_df, ...)`：用 aggTrades（推荐，真 VP）。
     trades_df 至少含 `price`, `amount`，可选 `side` ∈ {'buy','sell'} 用于
     拆分主动买/卖量。
   - `build_from_ohlcv(klines_df, ...)`：fallback 近似——把每根 K 的 volume
     均分到 [low, high] 区间所覆盖的 bin。**仅当无 trades 数据时使用**，
     输出 source='ohlcv_approx'，调用方应在文档里打 approx 标签。
3. **结果不可变**：返回 dataclass `VolumeProfile`，包含 bins DataFrame +
   关键水平价（POC / VAH / VAL / HVN / LVN）+ 元数据。
4. **窗口规则不在此处**：见 `config/defaults.yaml :: volume_profile` 与
   调用方（review_builder / day_review_builder）。

运行示例
--------
>>> from tools.volume_profile import build_from_trades
>>> vp = build_from_trades(trades_df, n_bins=80, va_pct=0.7)
>>> vp.poc, vp.va_low, vp.va_high
(41.21, 41.18, 41.27)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class VolumeProfile:
    """一段窗口内的成交量价格分布。"""

    bins: pd.DataFrame  # cols: bin_idx, price_lo, price_hi, mid, vol, buy_vol, sell_vol
    poc: float          # Point of Control：成交量最大 bin 的中价
    va_low: float       # Value Area 下沿
    va_high: float      # Value Area 上沿
    hvn: list[float] = field(default_factory=list)  # 高量节点中价（按 vol 降序）
    lvn: list[float] = field(default_factory=list)  # 低量节点中价（按 vol 升序）
    total_vol: float = 0.0
    source: str = "trades"  # "trades" | "ohlcv_approx"
    window: dict = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────────────────────────────
def build_from_trades(
    trades: pd.DataFrame,
    *,
    n_bins: int = 80,
    price_lo: Optional[float] = None,
    price_hi: Optional[float] = None,
    va_pct: float = 0.7,
    hvn_quantile: float = 0.80,
    lvn_quantile: float = 0.20,
) -> VolumeProfile:
    """从 aggTrades 构建 VP。

    trades 列至少包含 `price`, `amount`；`side` ∈ {'buy','sell'} 可选。
    """
    if trades is None or trades.empty:
        raise ValueError("trades 为空，无法构建 VP")
    if "price" not in trades.columns or "amount" not in trades.columns:
        raise ValueError("trades 必须包含 price 与 amount 两列")

    price = trades["price"].to_numpy(dtype=float)
    amount = trades["amount"].to_numpy(dtype=float)
    side = trades["side"].to_numpy() if "side" in trades.columns else None

    lo = float(np.min(price)) if price_lo is None else float(price_lo)
    hi = float(np.max(price)) if price_hi is None else float(price_hi)
    if hi <= lo:
        raise ValueError(f"价格区间无效: lo={lo} hi={hi}")

    edges = np.linspace(lo, hi, n_bins + 1)
    # 把每笔 trade 的 amount 累加到所在 bin
    bin_idx = np.clip(np.searchsorted(edges, price, side="right") - 1, 0, n_bins - 1)

    vol = np.bincount(bin_idx, weights=amount, minlength=n_bins)
    if side is not None:
        buy_mask = side == "buy"
        buy_vol = np.bincount(bin_idx[buy_mask], weights=amount[buy_mask], minlength=n_bins)
        sell_mask = side == "sell"
        sell_vol = np.bincount(bin_idx[sell_mask], weights=amount[sell_mask], minlength=n_bins)
    else:
        buy_vol = np.zeros(n_bins)
        sell_vol = np.zeros(n_bins)

    bins_df = _make_bins_df(edges, vol, buy_vol, sell_vol)
    return _finalize(
        bins_df,
        va_pct=va_pct,
        hvn_quantile=hvn_quantile,
        lvn_quantile=lvn_quantile,
        source="trades",
        window={"price_lo": lo, "price_hi": hi, "n_bins": n_bins, "n_trades": int(len(trades))},
    )


def build_from_ohlcv(
    klines: pd.DataFrame,
    *,
    n_bins: int = 80,
    price_lo: Optional[float] = None,
    price_hi: Optional[float] = None,
    va_pct: float = 0.7,
    hvn_quantile: float = 0.80,
    lvn_quantile: float = 0.20,
) -> VolumeProfile:
    """无 trades 时的近似 VP：把每根 K 的 volume 按 [low, high] 均分到所覆盖的 bin。

    仅作为 fallback；返回 source='ohlcv_approx'。
    """
    if klines is None or klines.empty:
        raise ValueError("klines 为空")
    for c in ("low", "high", "volume"):
        if c not in klines.columns:
            raise ValueError(f"klines 缺少列: {c}")

    lo = float(klines["low"].min()) if price_lo is None else float(price_lo)
    hi = float(klines["high"].max()) if price_hi is None else float(price_hi)
    if hi <= lo:
        raise ValueError(f"价格区间无效: lo={lo} hi={hi}")

    edges = np.linspace(lo, hi, n_bins + 1)
    vol = np.zeros(n_bins, dtype=float)

    for low, high, v in zip(
        klines["low"].to_numpy(dtype=float),
        klines["high"].to_numpy(dtype=float),
        klines["volume"].to_numpy(dtype=float),
    ):
        if v <= 0 or high <= low:
            continue
        # 找到该 K 覆盖的所有 bin，按重叠比例分配
        i0 = max(0, int(np.searchsorted(edges, low, side="right") - 1))
        i1 = min(n_bins - 1, int(np.searchsorted(edges, high, side="right") - 1))
        if i1 < i0:
            continue
        # 每个 bin 与 [low, high] 的重叠长度作为权重
        seg = np.empty(i1 - i0 + 1)
        for k, b in enumerate(range(i0, i1 + 1)):
            seg[k] = max(0.0, min(edges[b + 1], high) - max(edges[b], low))
        s = seg.sum()
        if s <= 0:
            continue
        vol[i0:i1 + 1] += v * seg / s

    bins_df = _make_bins_df(edges, vol, np.zeros(n_bins), np.zeros(n_bins))
    return _finalize(
        bins_df,
        va_pct=va_pct,
        hvn_quantile=hvn_quantile,
        lvn_quantile=lvn_quantile,
        source="ohlcv_approx",
        window={"price_lo": lo, "price_hi": hi, "n_bins": n_bins, "n_bars": int(len(klines))},
    )


# ─────────────────────────────────────────────────────────────────────
# 辅助
# ─────────────────────────────────────────────────────────────────────
def _make_bins_df(
    edges: np.ndarray, vol: np.ndarray, buy_vol: np.ndarray, sell_vol: np.ndarray
) -> pd.DataFrame:
    n = len(vol)
    return pd.DataFrame(
        {
            "bin_idx": np.arange(n),
            "price_lo": edges[:-1],
            "price_hi": edges[1:],
            "mid": (edges[:-1] + edges[1:]) / 2,
            "vol": vol,
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
        }
    )


def value_area(bins_df: pd.DataFrame, va_pct: float = 0.7) -> tuple[float, float, int]:
    """单 bin 贪心扩张的 70% Value Area。

    返回 (va_low, va_high, poc_bin_idx)。
    """
    vol = bins_df["vol"].to_numpy()
    if vol.sum() <= 0:
        # 无成交 → 整个范围当作 VA
        return float(bins_df["price_lo"].iloc[0]), float(bins_df["price_hi"].iloc[-1]), 0
    poc_idx = int(np.argmax(vol))
    target = vol.sum() * va_pct
    cum = vol[poc_idx]
    top, bot = poc_idx, poc_idx
    n = len(vol)
    while cum < target:
        up = vol[top + 1] if top + 1 < n else -1.0
        dn = vol[bot - 1] if bot - 1 >= 0 else -1.0
        if up < 0 and dn < 0:
            break
        if up >= dn:
            top += 1
            cum += max(up, 0.0)
        else:
            bot -= 1
            cum += max(dn, 0.0)
    va_low = float(bins_df["price_lo"].iloc[bot])
    va_high = float(bins_df["price_hi"].iloc[top])
    return va_low, va_high, poc_idx


def _local_peaks(vol: np.ndarray) -> list[int]:
    """3-bin 平滑后严格本地峰值 bin 索引。"""
    if len(vol) < 3:
        return []
    smooth = pd.Series(vol).rolling(3, center=True, min_periods=1).mean().to_numpy()
    return [
        i for i in range(1, len(smooth) - 1)
        if smooth[i] > smooth[i - 1] and smooth[i] > smooth[i + 1]
    ]


def _lvn_between_peaks(vol: np.ndarray, peak_indices: list[int]) -> list[int]:
    """每对相邻峰之间取量最小的 bin 作为 LVN（即"快速穿越带"）。

    若两峰之间出现连续等量段（典型为全 0），取该段中点。
    """
    if len(peak_indices) < 2:
        return []
    out: list[int] = []
    sorted_peaks = sorted(peak_indices)
    for a, b in zip(sorted_peaks[:-1], sorted_peaks[1:]):
        if b - a < 2:
            continue
        between = vol[a + 1:b]
        if len(between) == 0:
            continue
        min_v = between.min()
        # 取所有等于最小值的 bin，挑中位位置作为代表
        candidates = np.where(between == min_v)[0]
        rep = int(candidates[len(candidates) // 2]) + a + 1
        out.append(rep)
    return out


def _finalize(
    bins_df: pd.DataFrame,
    *,
    va_pct: float,
    hvn_quantile: float,
    lvn_quantile: float,
    source: str,
    window: dict,
) -> VolumeProfile:
    vol = bins_df["vol"].to_numpy()
    total = float(vol.sum())
    va_low, va_high, poc_idx = value_area(bins_df, va_pct=va_pct)
    poc = float(bins_df["mid"].iloc[poc_idx])

    # HVN：本地峰值 ∩ 量 ≥ hvn_quantile 分位（仅在有量的 bin 间取分位）
    nz = vol[vol > 0]
    if len(nz) > 0:
        hvn_thr = float(np.quantile(nz, hvn_quantile))
        lvn_thr = float(np.quantile(nz, lvn_quantile))
    else:
        hvn_thr = float("inf")
        lvn_thr = -float("inf")

    peaks = _local_peaks(vol)
    hvn = sorted(
        [int(i) for i in peaks if vol[i] >= hvn_thr],
        key=lambda i: -vol[i],
    )
    # LVN 取相邻 HVN 之间最小量 bin（含 0 量），符合"快速穿越带"语义
    lvn = _lvn_between_peaks(vol, hvn)
    # 进一步过滤：lvn bin 量必须显著低于 lvn_thr，否则两峰间没有"空隙"
    lvn = [i for i in lvn if vol[i] <= lvn_thr]

    hvn_prices = [float(bins_df["mid"].iloc[i]) for i in hvn]
    lvn_prices = [float(bins_df["mid"].iloc[i]) for i in lvn]

    return VolumeProfile(
        bins=bins_df,
        poc=poc,
        va_low=va_low,
        va_high=va_high,
        hvn=hvn_prices,
        lvn=lvn_prices,
        total_vol=total,
        source=source,
        window=window,
    )
