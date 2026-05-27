"""NEAR 5/27 两笔 5m short 复盘分析（事件1: 16:20 / 事件2: 19:10）。

目的：
1) 核验两笔锚 K OHLC 与用户描述一致
2) 取出 5m / 15m 周边特征：EMA、wave、wick、vol_vs_ma、delta、cvd、ema_relation、regime
3) 给出事件2"系统合规性"判定的关键字段输出（剔除后验）
"""
from __future__ import annotations

import pandas as pd

from tools.validators import (
    load_day,
    get_bar,
    detect_features,
    regime_summary,
    regime_segments,
    ema_relation,
)

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", None)


def show(df: pd.DataFrame, t0: str, t1: str, cols=None) -> None:
    sub = df[(df["datetime"] >= t0) & (df["datetime"] <= t1)].copy()
    cols = cols or [
        "id",
        "datetime",
        "open",
        "high",
        "low",
        "close",
        "vol_vs_ma",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "body_ratio",
        "wave",
        "ema_21",
        "ema_55",
        "ema_100",
        "ema_200",
    ]
    cols = [c for c in cols if c in sub.columns]
    print(sub[cols].to_string(index=False))


def show_of(df: pd.DataFrame, t0: str, t1: str) -> None:
    cols = ["id", "datetime", "close", "volume", "delta", "buy_ratio", "cvd"]
    sub = df[(df["datetime"] >= t0) & (df["datetime"] <= t1)].copy()
    cols = [c for c in cols if c in sub.columns]
    print(sub[cols].to_string(index=False))


def main() -> None:
    date = "2026-05-27"
    df5 = load_day(date, "NEAR", "5m")
    # supp 15m 直接读 parquet（load_day 只支持 day_ 前缀）
    from pathlib import Path
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    supp_path = PROJECT_ROOT / "reviews" / date / "data" / "supp_NEAR_15m.parquet"
    df15 = pd.read_parquet(supp_path).sort_values("timestamp").reset_index(drop=True)
    df15["datetime"] = pd.to_datetime(df15["datetime"])

    print("=" * 100)
    print("[STEP 0] 5m / 15m regime 概览")
    print("=" * 100)
    print("5m regime_summary:", regime_summary(df5))
    print("5m regime_segments:")
    for seg in regime_segments(df5):
        print(" ", seg)
    print("15m regime_summary:", regime_summary(df15))
    print("15m regime_segments:")
    for seg in regime_segments(df15):
        print(" ", seg)

    # ========== 事件1 锚 K：5m 16:20 ==========
    print("\n" + "=" * 100)
    print("[EVENT 1] 5m 16:20 锚 K 与前后 6 根（共 13 根）")
    print("=" * 100)
    show(df5, "2026-05-27 15:50", "2026-05-27 16:50")
    print("\n[EVENT 1] 5m 16:20 订单流（前后 30min）")
    show_of(df5, "2026-05-27 15:50", "2026-05-27 16:50")

    bar_5m_1620 = df5[df5["datetime"] == "2026-05-27 16:20"].iloc[0]
    print("\n[EVENT 1] 5m 16:20 detect_features:")
    feats = detect_features(bar_5m_1620)
    print(" patterns:", feats.patterns)
    print(" evidence:", feats.evidence)
    print(" ema_relation:", ema_relation(bar_5m_1620))

    print("\n[EVENT 1] 15m 周边（15:45-16:30，含锚 K 的 15m 容器 16:15-16:30）")
    show(df15, "2026-05-27 14:45", "2026-05-27 17:00")
    bar_15m_1615 = df15[df15["datetime"] == "2026-05-27 16:15"].iloc[0]
    print("\n[EVENT 1] 15m 16:15 detect_features + ema_relation:")
    print(" patterns:", detect_features(bar_15m_1615).patterns)
    print(" evidence:", detect_features(bar_15m_1615).evidence)
    print(" ema_relation:", ema_relation(bar_15m_1615))

    # ========== 事件2 锚 K：5m 19:10 ==========
    print("\n" + "=" * 100)
    print("[EVENT 2] 5m 19:10 锚 K 与前后 8 根")
    print("=" * 100)
    show(df5, "2026-05-27 18:30", "2026-05-27 19:50")
    print("\n[EVENT 2] 5m 19:10 订单流（前后 40min）")
    show_of(df5, "2026-05-27 18:30", "2026-05-27 19:50")

    bar_5m_1910 = df5[df5["datetime"] == "2026-05-27 19:10"].iloc[0]
    print("\n[EVENT 2] 5m 19:10 detect_features:")
    feats2 = detect_features(bar_5m_1910)
    print(" patterns:", feats2.patterns)
    print(" evidence:", feats2.evidence)
    print(" ema_relation:", ema_relation(bar_5m_1910))

    print("\n[EVENT 2] 15m 18:00-20:00")
    show(df15, "2026-05-27 18:00", "2026-05-27 20:00")
    bar_15m_1900 = df15[df15["datetime"] == "2026-05-27 19:00"].iloc[0]
    print("\n[EVENT 2] 15m 19:00 detect_features + ema_relation:")
    print(" patterns:", detect_features(bar_15m_1900).patterns)
    print(" evidence:", detect_features(bar_15m_1900).evidence)
    print(" ema_relation:", ema_relation(bar_15m_1900))

    # ========== 事件1：止盈/止损命中检查（仅事件1 后验，以确认参考） ==========
    print("\n" + "=" * 100)
    print("[POST] 事件1 16:20 之后 5m 走势直到 19:30")
    print("=" * 100)
    sub = df5[(df5["datetime"] >= "2026-05-27 16:20") & (df5["datetime"] <= "2026-05-27 19:30")]
    sub = sub[["id", "datetime", "open", "high", "low", "close"]]
    # 标记 take/stop
    take, stop = 2.527, 2.593
    sub = sub.assign(hit_take=sub["low"] <= take, hit_stop=sub["high"] >= stop)
    print(sub.to_string(index=False))

    print("\n" + "=" * 100)
    print("[POST] 事件2 19:10 之后 5m 走势直到 22:00")
    print("=" * 100)
    sub2 = df5[(df5["datetime"] >= "2026-05-27 19:10") & (df5["datetime"] <= "2026-05-27 22:00")]
    sub2 = sub2[["id", "datetime", "open", "high", "low", "close"]]
    take2, stop2 = 2.494, 2.554
    sub2 = sub2.assign(hit_take=sub2["low"] <= take2, hit_stop=sub2["high"] >= stop2)
    print(sub2.to_string(index=False))


if __name__ == "__main__":
    main()
