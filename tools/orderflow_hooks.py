"""订单流热点钩子引擎。

设计目标
========
"全局纵览，局部重点关注"——对一段窗口内的 K 线，bar-level delta/cvd 始终来自
klines.taker_buy_base（足够便宜、零分页风险）；只有"形态可疑"的 K 线才拉取
aggTrades 以获取微观证据（footprint / 大单 / sub-bar tape）。

钩子（hook）= 一组可配置的判定函数，输入是带 delta 列的 K 线 DataFrame，
输出是命中的 bar 索引集合。多个钩子的命中结果取并集，再加 buffer，再合并相邻
连续段，最终得到要拉 aggTrades 的时间窗口列表。

支持的钩子
==========
- ``anchor``                锚 K（kline_id == "A0" / 由调用方传入）
- ``long_upper_wick``       upper_wick_ratio ≥ long_wick
- ``long_lower_wick``       lower_wick_ratio ≥ long_wick
- ``absorption_candidate``  阳线 + Δ < 0 或 阴线 + Δ > 0，且 |Δ|/vol ≥ absorption_delta_pct
- ``engulf``                engulf ∈ {bull, bear}
- ``wave_extreme``          |wave| ≥ wave_extreme
- ``high_volume``           vol_vs_ma ≥ high_volume
- ``bar_ids=[...]``         显式指定（通过 hook_extra_bar_ids 参数传入）

阈值见 `config/defaults.yaml > orderflow.hook_thresholds`。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

import pandas as pd


DEFAULT_THRESHOLDS = {
    "long_wick": 0.4,
    "absorption_delta_pct": 0.05,
    "wave_extreme": 40.0,
    "high_volume": 1.5,
}


@dataclass
class HotspotResult:
    """钩子引擎输出。"""
    hotspot_bar_ids: List[str] = field(default_factory=list)
    matches: Dict[str, List[str]] = field(default_factory=dict)   # bar_id -> [hook_name, ...]
    hooks_used: List[str] = field(default_factory=list)
    thresholds: Dict[str, float] = field(default_factory=dict)
    buffer_bars: int = 0
    expanded_bar_ids: List[str] = field(default_factory=list)     # 含 buffer 的最终列表
    contiguous_ranges: List[Dict] = field(default_factory=list)   # [{start_ts, end_ts, bar_count}, ...]
    klines_total: int = 0


# -------- 单个钩子实现（df 必须有 delta 列；其他列见 detect_features） --------

def _hook_anchor(df: pd.DataFrame, anchor_bar_id: Optional[str]) -> Set[str]:
    if not anchor_bar_id:
        return set()
    mask = df["kline_id"].astype(str) == str(anchor_bar_id)
    return set(df.loc[mask, "kline_id"].astype(str))


def _hook_long_upper_wick(df: pd.DataFrame, th: Dict[str, float]) -> Set[str]:
    if "upper_wick_ratio" not in df.columns:
        return set()
    return set(df.loc[df["upper_wick_ratio"] >= th["long_wick"], "kline_id"].astype(str))


def _hook_long_lower_wick(df: pd.DataFrame, th: Dict[str, float]) -> Set[str]:
    if "lower_wick_ratio" not in df.columns:
        return set()
    return set(df.loc[df["lower_wick_ratio"] >= th["long_wick"], "kline_id"].astype(str))


def _hook_absorption(df: pd.DataFrame, th: Dict[str, float]) -> Set[str]:
    """阳线 + Δ负 或 阴线 + Δ正，且 |Δ|/vol 比值不能太小（否则是噪点）。"""
    if "delta" not in df.columns or "is_bull" not in df.columns or "volume" not in df.columns:
        return set()
    vol = df["volume"].where(df["volume"] > 0, 1)
    ratio = (df["delta"].abs() / vol).fillna(0)
    bull_abs = (df["is_bull"].astype(bool)) & (df["delta"] < 0) & (ratio >= th["absorption_delta_pct"])
    bear_abs = (~df["is_bull"].astype(bool)) & (df["delta"] > 0) & (ratio >= th["absorption_delta_pct"])
    mask = bull_abs | bear_abs
    return set(df.loc[mask, "kline_id"].astype(str))


def _hook_engulf(df: pd.DataFrame) -> Set[str]:
    if "engulf" not in df.columns:
        return set()
    mask = df["engulf"].astype(str).isin(["bull", "bear"])
    return set(df.loc[mask, "kline_id"].astype(str))


def _hook_wave_extreme(df: pd.DataFrame, th: Dict[str, float]) -> Set[str]:
    if "wave" not in df.columns:
        return set()
    mask = df["wave"].abs() >= th["wave_extreme"]
    return set(df.loc[mask, "kline_id"].astype(str))


def _hook_high_volume(df: pd.DataFrame, th: Dict[str, float]) -> Set[str]:
    if "vol_vs_ma" not in df.columns:
        return set()
    mask = df["vol_vs_ma"] >= th["high_volume"]
    return set(df.loc[mask, "kline_id"].astype(str))


_HOOK_REGISTRY = {
    "long_upper_wick": _hook_long_upper_wick,
    "long_lower_wick": _hook_long_lower_wick,
    "absorption_candidate": _hook_absorption,
    "engulf": _hook_engulf,
    "wave_extreme": _hook_wave_extreme,
    "high_volume": _hook_high_volume,
}


# -------- 主入口 --------

def detect_hotspots(
    klines: pd.DataFrame,
    hooks: Iterable[str],
    *,
    anchor_bar_id: Optional[str] = None,
    extra_bar_ids: Optional[Iterable[str]] = None,
    buffer_bars: int = 1,
    thresholds: Optional[Dict[str, float]] = None,
    tf_ms: Optional[int] = None,
) -> HotspotResult:
    """识别热点 bar 并扩展 buffer，再合并为连续时间段。

    Args:
        klines: 必须包含 kline_id / timestamp / volume / delta（如要 absorption）
                / upper_wick_ratio / lower_wick_ratio / is_bull / engulf / wave / vol_vs_ma
        hooks: 要启用的钩子名列表，未知钩子会被忽略并打印告警
        anchor_bar_id: ``anchor`` 钩子使用（不必出现在 hooks 列表里——传了就生效）
        extra_bar_ids: 显式追加的 bar_id（如 yaml 里手工指定）
        buffer_bars: 热点 bar 前后各 ±N 根
        thresholds: 阈值覆盖 DEFAULT_THRESHOLDS
        tf_ms: K 周期毫秒数；用于生成 contiguous_ranges 的 start_ts/end_ts
    Returns:
        HotspotResult
    """
    th = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    klines = klines.reset_index(drop=True).copy()
    klines["kline_id"] = klines["kline_id"].astype(str)

    matches: Dict[str, Set[str]] = {}

    if anchor_bar_id:
        ids = _hook_anchor(klines, anchor_bar_id)
        if ids:
            for bid in ids:
                matches.setdefault(bid, set()).add("anchor")

    hooks_used: List[str] = []
    for hook in hooks:
        if hook == "anchor":
            # anchor 由 anchor_bar_id 参数驱动，已处理
            hooks_used.append("anchor")
            continue
        fn = _HOOK_REGISTRY.get(hook)
        if not fn:
            print(f"[hooks] 未知钩子: {hook}, 忽略")
            continue
        hooks_used.append(hook)
        if hook in ("long_upper_wick", "long_lower_wick", "absorption_candidate",
                    "wave_extreme", "high_volume"):
            ids = fn(klines, th)
        else:
            ids = fn(klines)
        for bid in ids:
            matches.setdefault(bid, set()).add(hook)

    if extra_bar_ids:
        for bid in extra_bar_ids:
            matches.setdefault(str(bid), set()).add("explicit")

    hotspot_bar_ids = sorted(
        matches.keys(),
        key=lambda b: int(klines.index[klines["kline_id"] == b][0]) if (klines["kline_id"] == b).any() else 0,
    )

    # buffer 扩展（按窗口索引展开）
    expanded: Set[int] = set()
    for bid in hotspot_bar_ids:
        idx_match = klines.index[klines["kline_id"] == bid]
        if len(idx_match) == 0:
            continue
        i = int(idx_match[0])
        for j in range(max(0, i - buffer_bars), min(len(klines), i + buffer_bars + 1)):
            expanded.add(j)
    expanded_idx_sorted = sorted(expanded)
    expanded_bar_ids = klines.loc[expanded_idx_sorted, "kline_id"].astype(str).tolist()

    # 合并连续段（基于索引相邻）
    ranges: List[Dict] = []
    if expanded_idx_sorted and tf_ms:
        run_start = expanded_idx_sorted[0]
        prev = run_start
        for i in expanded_idx_sorted[1:] + [None]:
            if i is None or i != prev + 1:
                start_ts = int(klines.iloc[run_start]["timestamp"])
                end_ts = int(klines.iloc[prev]["timestamp"]) + tf_ms  # 闭右开
                ranges.append({
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "bar_count": prev - run_start + 1,
                    "first_bar_id": str(klines.iloc[run_start]["kline_id"]),
                    "last_bar_id": str(klines.iloc[prev]["kline_id"]),
                })
                if i is not None:
                    run_start = i
            if i is not None:
                prev = i

    return HotspotResult(
        hotspot_bar_ids=hotspot_bar_ids,
        matches={k: sorted(v) for k, v in matches.items()},
        hooks_used=hooks_used,
        thresholds=th,
        buffer_bars=buffer_bars,
        expanded_bar_ids=expanded_bar_ids,
        contiguous_ranges=ranges,
        klines_total=len(klines),
    )
