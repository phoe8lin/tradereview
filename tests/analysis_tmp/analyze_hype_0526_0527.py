"""分析 HYPE/USDT 永续 2026-05-24 ~ 2026-05-27 的 1h / 5m 行情，
为两笔做空交易（0526 22:00 / 0527 20:20 限价）的入场直觉提供数据支撑。
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import pandas as pd

from tools.data_fetcher import FetchSpec, fetch_around_anchor
from tools.indicators import add_ema, add_wave_filter, classify_ema_stack
from tools.kline_features import add_kline_features

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 30)
pd.set_option("display.float_format", lambda x: f"{x:.4f}")

OUT_DIR = Path(__file__).resolve().parent / "hype_0526_0527_out"
OUT_DIR.mkdir(parents=True, exist_ok=True)


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    add_ema(df, periods=(21, 55, 100, 200))
    add_wave_filter(df)
    add_kline_features(df)
    df["ema_stack"] = df.apply(classify_ema_stack, axis=1)
    return df


def fmt(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df[cols].copy()
    return out


def main() -> None:
    # ---- 1h: 覆盖 5/14 ~ 5/27 23:00（EMA200 需要足够预热）----
    spec_1h = FetchSpec(
        exchange="binance",
        market="futures",
        base="HYPE",
        quote="USDT",
        timeframe="1h",
        anchor_cn="2026-05-26 22:00",
        bars_before=300,   # 300h ≈ 12.5d 预热
        bars_after=48,     # 到 5/28 22:00
    )
    df_1h = fetch_around_anchor(spec_1h)
    df_1h = enrich(df_1h)
    df_1h.to_parquet(OUT_DIR / "hype_1h.parquet", index=False)

    # 截取 5/22 00:00 ~ 5/28 00:00 显示
    mask = (df_1h["datetime"] >= "2026-05-22 00:00") & (df_1h["datetime"] < "2026-05-28 00:00")
    show_1h = df_1h.loc[mask].copy()
    show_1h["datetime"] = show_1h["datetime"].dt.strftime("%m-%d %H:%M")
    cols_1h = [
        "datetime", "open", "high", "low", "close", "volume",
        "vol_vs_ma", "body_ratio", "upper_wick_ratio", "lower_wick_ratio",
        "engulf", "wave", "ema_21", "ema_55", "ema_100", "ema_200", "ema_stack",
    ]
    print("=" * 100)
    print("HYPE/USDT perp 1h  2026-05-22 ~ 05-27")
    print("=" * 100)
    print(fmt(show_1h, cols_1h).to_string(index=False))
    show_1h.to_csv(OUT_DIR / "hype_1h_show.csv", index=False)

    # ---- 5m: 覆盖 5/26 12:00 ~ 5/28 00:00 ----
    spec_5m = FetchSpec(
        exchange="binance",
        market="futures",
        base="HYPE",
        quote="USDT",
        timeframe="5m",
        anchor_cn="2026-05-27 20:20",
        bars_before=600,   # 50h
        bars_after=120,    # +10h
    )
    df_5m = fetch_around_anchor(spec_5m)
    df_5m = enrich(df_5m)
    df_5m.to_parquet(OUT_DIR / "hype_5m.parquet", index=False)

    # 5/27 全天 5m
    mask5 = (df_5m["datetime"] >= "2026-05-27 00:00") & (df_5m["datetime"] < "2026-05-28 00:00")
    show_5m = df_5m.loc[mask5].copy()
    show_5m["datetime"] = show_5m["datetime"].dt.strftime("%m-%d %H:%M")
    cols_5m = [
        "datetime", "open", "high", "low", "close", "volume",
        "vol_vs_ma", "body_ratio", "upper_wick_ratio", "lower_wick_ratio",
        "engulf", "wave", "ema_21", "ema_55", "ema_200", "ema_stack",
    ]
    print()
    print("=" * 100)
    print("HYPE/USDT perp 5m  2026-05-27 全天")
    print("=" * 100)
    print(fmt(show_5m, cols_5m).to_string(index=False))
    show_5m.to_csv(OUT_DIR / "hype_5m_show.csv", index=False)

    # ---- 关键统计 ----
    print()
    print("=" * 100)
    print("关键 K 线核验")
    print("=" * 100)

    # 5/26 22:00 1h
    row = df_1h[df_1h["datetime"] == pd.Timestamp("2026-05-26 22:00", tz="Asia/Shanghai")]
    if len(row):
        r = row.iloc[0]
        print(f"\n[1h] 05-26 22:00  O={r.open:.3f} H={r.high:.3f} L={r.low:.3f} C={r.close:.3f}  "
              f"upper_wick={r.upper_wick_ratio:.2f}  body={r.body_ratio:.2f}  "
              f"vol_vs_ma={r.vol_vs_ma:.2f}  wave={r.wave:.2f}  ema_stack={r.ema_stack}  "
              f"engulf={r.engulf}")

    # 5/27 17/18/19 1h
    for hh in ("17:00", "18:00", "19:00", "20:00", "21:00"):
        row = df_1h[df_1h["datetime"] == pd.Timestamp(f"2026-05-27 {hh}", tz="Asia/Shanghai")]
        if len(row):
            r = row.iloc[0]
            print(f"[1h] 05-27 {hh}  O={r.open:.3f} H={r.high:.3f} L={r.low:.3f} C={r.close:.3f}  "
                  f"upper_wick={r.upper_wick_ratio:.2f}  body={r.body_ratio:.2f}  "
                  f"vol_vs_ma={r.vol_vs_ma:.2f}  wave={r.wave:.2f}  ema_stack={r.ema_stack}  "
                  f"engulf={r.engulf}")

    # 5/27 20:20 5m
    row = df_5m[df_5m["datetime"] == pd.Timestamp("2026-05-27 20:20", tz="Asia/Shanghai")]
    if len(row):
        r = row.iloc[0]
        print(f"\n[5m] 05-27 20:20  O={r.open:.3f} H={r.high:.3f} L={r.low:.3f} C={r.close:.3f}  "
              f"upper_wick={r.upper_wick_ratio:.2f}  body={r.body_ratio:.2f}  "
              f"vol_vs_ma={r.vol_vs_ma:.2f}  wave={r.wave:.2f}  ema_stack={r.ema_stack}")

    # 5/26 22:00 之后 1h 表现（验证第一单）
    print("\n[1h] 5/26 22:00 之后 24h 行情（验证第一单走势）：")
    post = df_1h[(df_1h["datetime"] >= "2026-05-26 22:00") & (df_1h["datetime"] < "2026-05-28 00:00")].copy()
    print(f"  min low after entry = {post['low'].min():.3f}")
    print(f"  max high after entry = {post['high'].max():.3f}")

    # 5/24 ~ 5/26 上沿测试统计：高点>=63.5 的 1h
    test_zone = df_1h[(df_1h["datetime"] >= "2026-05-24 00:00") & (df_1h["datetime"] < "2026-05-27 00:00")]
    test_zone_hits = test_zone[test_zone["high"] >= 63.5]
    print(f"\n[1h] 05-24~05-26 触及 63.5+ 的小时数: {len(test_zone_hits)} / {len(test_zone)}")
    print(test_zone_hits[["datetime", "high", "close", "upper_wick_ratio", "wave"]].assign(
        datetime=test_zone_hits["datetime"].dt.strftime("%m-%d %H:%M")
    ).to_string(index=False))

    # 5/27 17-20 整段窗口 5m 高点
    print("\n[5m] 05-27 17:00-20:25 5m 高点/收盘分布：")
    win5 = df_5m[(df_5m["datetime"] >= "2026-05-27 17:00") & (df_5m["datetime"] < "2026-05-27 20:30")].copy()
    win5["datetime"] = win5["datetime"].dt.strftime("%H:%M")
    print(win5[["datetime", "open", "high", "low", "close", "volume", "vol_vs_ma",
                "upper_wick_ratio", "wave"]].to_string(index=False))


if __name__ == "__main__":
    main()
