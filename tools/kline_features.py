"""单根 K 线形态特征：影线比例、实体比例、量能相对强度、ATR 相对波幅、吞没等。

所有字段原地新增到传入的 df，返回同一个 df。
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def add_kline_features(df: pd.DataFrame, vol_ma_window: int = 20, atr_period: int = 14) -> pd.DataFrame:
    o = df["open"]
    h = df["high"]
    l = df["low"]
    c = df["close"]

    rng = (h - l).replace(0, np.nan)
    body = (c - o).abs()
    upper_wick = h - c.combine(o, max)
    lower_wick = c.combine(o, min) - l

    df["body_ratio"] = (body / rng).clip(0, 1).fillna(0)
    df["upper_wick_ratio"] = (upper_wick / rng).clip(0, 1).fillna(0)
    df["lower_wick_ratio"] = (lower_wick / rng).clip(0, 1).fillna(0)
    df["is_bull"] = (c >= o).astype(int)

    # ATR(14)（Wilder RMA）
    tr = pd.concat(
        [
            h - l,
            (h - c.shift()).abs(),
            (l - c.shift()).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.ewm(alpha=1 / atr_period, adjust=False).mean()
    df["atr"] = atr
    df["range_vs_atr"] = ((h - l) / atr).replace([np.inf, -np.inf], np.nan)

    # 成交量相对强度
    vol_ma = df["volume"].rolling(vol_ma_window).mean()
    df["vol_ma"] = vol_ma
    df["vol_vs_ma"] = (df["volume"] / vol_ma).replace([np.inf, -np.inf], np.nan)

    # 简单吞没标记（看收盘相对前根实体）
    prev_o = o.shift(1)
    prev_c = c.shift(1)
    prev_body_high = pd.concat([prev_o, prev_c], axis=1).max(axis=1)
    prev_body_low = pd.concat([prev_o, prev_c], axis=1).min(axis=1)
    bull_engulf = (c > o) & (prev_c < prev_o) & (c >= prev_body_high) & (o <= prev_body_low)
    bear_engulf = (c < o) & (prev_c > prev_o) & (o >= prev_body_high) & (c <= prev_body_low)
    df["engulf"] = np.where(bull_engulf, "bull", np.where(bear_engulf, "bear", ""))

    return df
