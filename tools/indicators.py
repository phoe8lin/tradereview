"""指标计算：EMA 均线组 + Wave Filter（StochRSI 变种）

Wave Filter 与 pinescript指标/超买超卖过滤器.pine 完全对齐：
    RSI(src, period)
    StochRSI = (RSI - min(RSI, period)) / (max(RSI, period) - min(RSI, period))
    K = MA(StochRSI, smooth_k)
    D = MA(K, smooth_d)
    Wave = (D - 0.5) * 100   ∈ [-50, 50]
    超买 +40，超卖 -40
"""
from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd


# ---- EMA ----
def add_ema(df: pd.DataFrame, periods: Iterable[int], source: str = "close") -> pd.DataFrame:
    """原地新增 ema_{period} 列，返回同一个 df。"""
    for p in periods:
        df[f"ema_{p}"] = df[source].ewm(span=p, adjust=False).mean()
    return df


def classify_ema_stack(row: pd.Series, periods=(21, 55, 100, 200)) -> str:
    """根据 EMA 组大小关系给出排列标签。"""
    vals = [row.get(f"ema_{p}") for p in periods]
    if any(pd.isna(v) for v in vals):
        return "unknown"
    # 多头：21>55>100>200
    if all(vals[i] > vals[i + 1] for i in range(len(vals) - 1)):
        return "bull_stack"
    # 空头：21<55<100<200
    if all(vals[i] < vals[i + 1] for i in range(len(vals) - 1)):
        return "bear_stack"
    return "tangled"


# ---- Wave Filter ----
def _ma(series: pd.Series, length: int, ma_type: str) -> pd.Series:
    t = ma_type.upper()
    if t == "SMA":
        return series.rolling(window=length).mean()
    if t == "EMA":
        return series.ewm(span=length, adjust=False).mean()
    if t == "RMA":
        # Wilder smoothing
        return series.ewm(alpha=1 / length, adjust=False).mean()
    if t == "WMA":
        weights = np.arange(1, length + 1, dtype=float)
        return series.rolling(length).apply(
            lambda x: np.dot(x, weights) / weights.sum(), raw=True
        )
    raise ValueError(f"unsupported ma_type: {ma_type}")


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """与 Pine ta.rsi 一致：Wilder RMA 平滑。"""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - 100 / (1 + rs)
    return rsi


def add_wave_filter(
    df: pd.DataFrame,
    rsi_period: int = 14,
    stoch_period: int = 14,
    smooth_k: int = 3,
    smooth_d: int = 3,
    ma_type: str = "SMA",
    source: str = "close",
) -> pd.DataFrame:
    """原地新增 rsi / stoch_rsi / wave_k / wave_d / wave 列。wave 为主信号。"""
    rsi = calculate_rsi(df[source], rsi_period)
    min_rsi = rsi.rolling(stoch_period).min()
    max_rsi = rsi.rolling(stoch_period).max()
    denom = (max_rsi - min_rsi).replace(0, np.nan)
    stoch_rsi = (rsi - min_rsi) / denom
    stoch_rsi = stoch_rsi.fillna(0.5)

    k = _ma(stoch_rsi, smooth_k, ma_type)
    d = _ma(k, smooth_d, ma_type)
    wave = (d - 0.5) * 100

    df["rsi"] = rsi
    df["stoch_rsi"] = stoch_rsi
    df["wave_k"] = (k - 0.5) * 100
    df["wave_d"] = wave
    df["wave"] = wave
    return df


def classify_wave_state(wave: float, ob: float = 40, os_: float = -40) -> str:
    if pd.isna(wave):
        return "unknown"
    if wave >= ob:
        return "overbought"
    if wave <= os_:
        return "oversold"
    return "neutral"
