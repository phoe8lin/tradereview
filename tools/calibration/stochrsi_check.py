"""Wave Filter 校准工具：打印锚定 K 线及其前后若干根的 Wave / RSI / EMA 值。

用法：
    /opt/anaconda3/envs/trade/bin/python -m tools.calibration.stochrsi_check \
        --data reviews/2026-04-19/data/BTC_1h_demo.parquet --window 5

与 TradingView 鼠标悬停读数对比：
- Wave Filter 主线值，绝对误差 < 1 视为通过（平滑差异带来微小偏移属正常）。
- 关键是超买/超卖区间与穿越时机一致。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data", required=True, help="parquet 路径")
    p.add_argument("--window", type=int, default=5, help="锚定 K 线前后展示多少根")
    args = p.parse_args()

    df = pd.read_parquet(args.data)
    anchor_idx = df.index[df["is_anchor"]]
    if len(anchor_idx) == 0:
        raise SystemExit("未找到 is_anchor=True 的行")
    a = int(anchor_idx[0])
    lo = max(0, a - args.window)
    hi = min(len(df) - 1, a + args.window)

    cols = [
        "kline_id", "datetime", "open", "high", "low", "close", "volume",
        "ema_21", "ema_55", "ema_100", "ema_200",
        "rsi", "wave", "upper_wick_ratio", "lower_wick_ratio", "body_ratio",
    ]
    view = df.loc[lo:hi, cols].copy()
    for c in ["ema_21", "ema_55", "ema_100", "ema_200", "rsi", "wave"]:
        view[c] = view[c].round(3)
    for c in ["upper_wick_ratio", "lower_wick_ratio", "body_ratio"]:
        view[c] = view[c].round(3)
    print(view.to_string(index=False))


if __name__ == "__main__":
    main()
