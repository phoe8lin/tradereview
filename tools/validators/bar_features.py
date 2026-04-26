"""K 线形态自动判定。

杜绝"看起来像长上影""感觉是 absorption"的凭印象判定。
所有形态都基于 parquet 已有字段 + 明确阈值 + 数值证据。

支持的形态（每个都返回 (matched: bool, evidence: dict)）：

- ``long_upper_wick``       上影 ≥ 0.4
- ``long_lower_wick``       下影 ≥ 0.4
- ``doji``                  实体 ≤ 0.15
- ``pin_bar_top``           上影 ≥ 0.6 + 实体 ≤ 0.2 + close 在下半区
- ``pin_bar_bottom``        下影 ≥ 0.6 + 实体 ≤ 0.2 + close 在上半区
- ``bull_absorption``       阳线 (is_bull=1) 但 delta < -threshold（阳吃卖盘）
- ``bear_absorption``       阴线 (is_bull=0) 但 delta > +threshold（阴吃买盘）
- ``breakout_bar``          range_vs_atr ≥ 1.5 且 close 在区间末端 20%
- ``engulf_bull`` / ``engulf_bear``   直接读 parquet 的 engulf 字段
- ``high_volume``           vol_vs_ma ≥ 1.8
- ``low_volume``            vol_vs_ma ≤ 0.6

对于 absorption，threshold 默认 1500（约 HYPE 5m 当日 |delta| 75% 分位）。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

import pandas as pd

from .data_loader import parse_ref


# 默认阈值集中在这里，方便统一调参。
DEFAULT_THRESHOLDS: Dict[str, float] = {
    "long_wick": 0.4,
    "doji_body": 0.15,
    "pin_wick": 0.6,
    "pin_body": 0.2,
    "absorption_delta": 1500.0,
    "breakout_atr": 1.5,
    "breakout_close_zone": 0.2,   # close 距区间端 ≤ 20%
    "high_vol": 1.8,
    "low_vol": 0.6,
}


@dataclass
class FeatureResult:
    bar_id: str
    time: str
    patterns: List[str] = field(default_factory=list)
    evidence: Dict[str, Dict] = field(default_factory=dict)

    def has(self, pattern: str) -> bool:
        return pattern in self.patterns

    def to_dict(self) -> Dict:
        return {
            "bar_id": self.bar_id,
            "time": self.time,
            "patterns": list(self.patterns),
            "evidence": dict(self.evidence),
        }


def _close_position_in_range(bar: pd.Series) -> float:
    """close 在 K 线区间中的相对位置 (0=最低, 1=最高)。"""
    h, l, c = float(bar["high"]), float(bar["low"]), float(bar["close"])
    rng = h - l
    if rng <= 0:
        return 0.5
    return (c - l) / rng


def detect_features(
    bar: pd.Series,
    thresholds: Optional[Dict] = None,
) -> FeatureResult:
    """对单根 K 线检测所有形态。"""
    th = dict(DEFAULT_THRESHOLDS)
    if thresholds:
        th.update(thresholds)

    upW = float(bar["upper_wick_ratio"])
    loW = float(bar["lower_wick_ratio"])
    body = float(bar["body_ratio"])
    is_bull = bool(bar["is_bull"])
    delta = float(bar["delta"])
    vol = float(bar["vol_vs_ma"])
    rng_atr = float(bar.get("range_vs_atr", 0.0)) if pd.notna(bar.get("range_vs_atr")) else 0.0
    engulf = str(bar.get("engulf") or "")
    pos = _close_position_in_range(bar)

    bar_id = str(bar["id"])
    bar_t = pd.to_datetime(bar["datetime"]).strftime("%H:%M") if "datetime" in bar else "?"
    res = FeatureResult(bar_id=bar_id, time=bar_t)

    # --- wick & body ---
    if upW >= th["long_wick"]:
        res.patterns.append("long_upper_wick")
        res.evidence["long_upper_wick"] = {"upper_wick_ratio": round(upW, 3), "threshold": th["long_wick"]}
    if loW >= th["long_wick"]:
        res.patterns.append("long_lower_wick")
        res.evidence["long_lower_wick"] = {"lower_wick_ratio": round(loW, 3), "threshold": th["long_wick"]}
    if body <= th["doji_body"]:
        res.patterns.append("doji")
        res.evidence["doji"] = {"body_ratio": round(body, 3), "threshold": th["doji_body"]}

    # --- pin bars ---
    if upW >= th["pin_wick"] and body <= th["pin_body"] and pos <= 0.5:
        res.patterns.append("pin_bar_top")
        res.evidence["pin_bar_top"] = {
            "upper_wick_ratio": round(upW, 3),
            "body_ratio": round(body, 3),
            "close_pos_in_range": round(pos, 3),
        }
    if loW >= th["pin_wick"] and body <= th["pin_body"] and pos >= 0.5:
        res.patterns.append("pin_bar_bottom")
        res.evidence["pin_bar_bottom"] = {
            "lower_wick_ratio": round(loW, 3),
            "body_ratio": round(body, 3),
            "close_pos_in_range": round(pos, 3),
        }

    # --- absorption (实体方向 vs delta 方向 相反) ---
    if is_bull and delta < -th["absorption_delta"]:
        res.patterns.append("bull_absorption")
        res.evidence["bull_absorption"] = {
            "is_bull": True,
            "delta": round(delta, 1),
            "threshold": -th["absorption_delta"],
            "note": "阳线收盘，但主动卖盘占优——吸筹/反转候选",
        }
    if (not is_bull) and delta > th["absorption_delta"]:
        res.patterns.append("bear_absorption")
        res.evidence["bear_absorption"] = {
            "is_bull": False,
            "delta": round(delta, 1),
            "threshold": th["absorption_delta"],
            "note": "阴线收盘，但主动买盘占优——吸卖盘/反转候选",
        }

    # --- breakout bar ---
    if rng_atr >= th["breakout_atr"]:
        # close 在区间靠端 20%
        in_top = pos >= (1 - th["breakout_close_zone"])
        in_bot = pos <= th["breakout_close_zone"]
        if in_top or in_bot:
            res.patterns.append("breakout_bar")
            res.evidence["breakout_bar"] = {
                "range_vs_atr": round(rng_atr, 2),
                "close_pos_in_range": round(pos, 3),
                "direction": "up" if in_top else "down",
            }

    # --- engulf (从 parquet 字段) ---
    if engulf == "bull":
        res.patterns.append("engulf_bull")
        res.evidence["engulf_bull"] = {"source": "parquet.engulf"}
    elif engulf == "bear":
        res.patterns.append("engulf_bear")
        res.evidence["engulf_bear"] = {"source": "parquet.engulf"}

    # --- volume ---
    if vol >= th["high_vol"]:
        res.patterns.append("high_volume")
        res.evidence["high_volume"] = {"vol_vs_ma": round(vol, 3), "threshold": th["high_vol"]}
    if vol <= th["low_vol"]:
        res.patterns.append("low_volume")
        res.evidence["low_volume"] = {"vol_vs_ma": round(vol, 3), "threshold": th["low_vol"]}

    return res


def scan_features(
    df: pd.DataFrame,
    bar_range: Optional[str] = None,
    patterns: Optional[List[str]] = None,
    thresholds: Optional[Dict] = None,
) -> List[FeatureResult]:
    """扫描区间内所有 K 线，返回每根的形态。

    bar_range: 'B100:B130' 或 None (扫全 df)
    patterns: 仅保留命中至少一个指定形态的 K
    """
    if bar_range:
        a, b = bar_range.split(":")
        i0 = parse_ref(df, a.strip())
        i1 = parse_ref(df, b.strip())
        sub = df.iloc[i0:i1 + 1]
    else:
        sub = df

    out: List[FeatureResult] = []
    for _, bar in sub.iterrows():
        r = detect_features(bar, thresholds)
        if patterns is None:
            out.append(r)
        elif any(p in r.patterns for p in patterns):
            out.append(r)
    return out


def format_feature_line(r: FeatureResult) -> str:
    if not r.patterns:
        return f"{r.bar_id} {r.time}: (无形态)"
    return f"{r.bar_id} {r.time}: " + ", ".join(r.patterns)
