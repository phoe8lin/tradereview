"""订单流查看工具：打印锚定 K 线前后若干根的 Delta / CVD / 买卖比。

用法：
    /opt/anaconda3/envs/trade/bin/python -m tools.calibration.orderflow_check \
        --date 2026-04-20 --trade-id HYPE_5m_001 --window 10

读取 reviews/<date>/data/<trade_id>.orderflow.parquet（需先用 orderflow_fetcher 生成），
并结合同目录的 <trade_id>.parquet 拿 OHLC + vol_vs_ma 作对比。
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from ..config import PROJECT_ROOT


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", required=True, help="复盘日期 YYYY-MM-DD")
    p.add_argument("--trade-id", required=True)
    p.add_argument("--window", type=int, default=10)
    args = p.parse_args()

    data_dir = PROJECT_ROOT / "reviews" / args.date / "data"
    kline_path = data_dir / f"{args.trade_id}.parquet"
    of_path = data_dir / f"{args.trade_id}.orderflow.parquet"
    if not of_path.exists():
        raise SystemExit(f"找不到 orderflow parquet: {of_path}\n 请先运行: python -m tools.orderflow_fetcher --date {args.date} --trade-id {args.trade_id}")

    kl = pd.read_parquet(kline_path)
    of = pd.read_parquet(of_path)

    # 找锚
    anchor_idx = kl.index[kl["is_anchor"]]
    if len(anchor_idx) == 0:
        raise SystemExit("未找到 is_anchor=True 的行")
    a_ts = int(kl.loc[int(anchor_idx[0]), "timestamp"])

    # 合并 OHLC / vol / vol_vs_ma 到 orderflow
    cols_kl = ["timestamp", "open", "high", "low", "close", "volume", "vol_vs_ma", "body_ratio", "upper_wick_ratio", "lower_wick_ratio"]
    merged = of.merge(kl[cols_kl], on="timestamp", how="left", suffixes=("", "_kl"))

    # 截窗
    a_i = merged.index[merged["timestamp"] == a_ts]
    if len(a_i) == 0:
        raise SystemExit("orderflow 中找不到锚定 K 线")
    a = int(a_i[0])
    lo = max(0, a - args.window)
    hi = min(len(merged) - 1, a + args.window)

    view = merged.loc[lo:hi].copy()
    view["★"] = view["timestamp"].apply(lambda t: "★" if t == a_ts else " ")
    show_cols = [
        "★", "kline_id", "datetime", "close",
        "volume", "vol_vs_ma", "buy_vol", "sell_vol",
        "delta", "cvd", "buy_ratio",
        "upper_wick_ratio", "lower_wick_ratio", "body_ratio",
    ]
    for c in ["buy_vol", "sell_vol", "delta", "cvd"]:
        view[c] = view[c].round(1)
    for c in ["buy_ratio", "vol_vs_ma", "upper_wick_ratio", "lower_wick_ratio", "body_ratio"]:
        view[c] = view[c].round(3)
    view["volume"] = view["volume"].round(1)
    print(view[show_cols].to_string(index=False))

    # 小结
    anchor_row = merged.loc[a]
    print()
    print(f"锚 K {anchor_row['kline_id']} @ {anchor_row['datetime']}:")
    print(f"  delta={anchor_row['delta']:.1f}  cvd={anchor_row['cvd']:.1f}  buy_ratio={anchor_row['buy_ratio']:.3f}  vol_vs_ma={anchor_row['vol_vs_ma']:.2f}")


if __name__ == "__main__":
    main()
