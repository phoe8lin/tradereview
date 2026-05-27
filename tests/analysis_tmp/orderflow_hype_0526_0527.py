"""为 5/26 22:00 1h short 与 5/27 20:20 5m short 拉订单流（klines taker_buy → delta/cvd）。
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import pandas as pd

from tools.orderflow_fetcher import fetch_klines_with_taker

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 40)
pd.set_option("display.float_format", lambda x: f"{x:.3f}")

OUT = Path(__file__).resolve().parent / "hype_0526_0527_out"
OUT.mkdir(parents=True, exist_ok=True)


def to_cn(ms):
    return pd.to_datetime(ms, unit="ms", utc=True).dt.tz_convert("Asia/Shanghai")


# ---------- 1h: 5/24 00:00 ~ 5/28 00:00 (UTC+8) ----------
start_1h = int(pd.Timestamp("2026-05-24 00:00", tz="Asia/Shanghai").tz_convert("UTC").timestamp() * 1000)
end_1h = int(pd.Timestamp("2026-05-28 00:00", tz="Asia/Shanghai").tz_convert("UTC").timestamp() * 1000)
k1h = fetch_klines_with_taker("futures", "HYPE", "USDT", "1h", start_1h, end_1h)
k1h["datetime"] = to_cn(k1h["timestamp"])
k1h["cvd"] = k1h["delta"].cumsum()
k1h.to_parquet(OUT / "hype_1h_orderflow.parquet", index=False)

# ---------- 5m: 5/27 14:00 ~ 5/27 23:00 ----------
start_5m = int(pd.Timestamp("2026-05-27 14:00", tz="Asia/Shanghai").tz_convert("UTC").timestamp() * 1000)
end_5m = int(pd.Timestamp("2026-05-27 23:00", tz="Asia/Shanghai").tz_convert("UTC").timestamp() * 1000)
k5m = fetch_klines_with_taker("futures", "HYPE", "USDT", "5m", start_5m, end_5m)
k5m["datetime"] = to_cn(k5m["timestamp"])
k5m["cvd"] = k5m["delta"].cumsum()
k5m.to_parquet(OUT / "hype_5m_orderflow.parquet", index=False)


def show(df, mask, cols, title):
    print()
    print("=" * 100)
    print(title)
    print("=" * 100)
    sub = df.loc[mask].copy()
    sub["datetime"] = sub["datetime"].dt.strftime("%m-%d %H:%M")
    print(sub[cols].to_string(index=False))


# 第一单 1h 上下文：5/26 18:00 ~ 5/27 06:00
m = (k1h["datetime"] >= "2026-05-26 18:00") & (k1h["datetime"] < "2026-05-27 06:00")
show(k1h, m, ["datetime", "open", "high", "low", "close", "volume",
              "taker_buy_base", "taker_sell_base", "delta", "buy_ratio", "cvd"],
     "1h orderflow — Trade1 上下文 (5/26 18:00 ~ 5/27 06:00)")

# 第二单 1h 上下文：5/27 14:00 ~ 5/27 22:00
m = (k1h["datetime"] >= "2026-05-27 14:00") & (k1h["datetime"] < "2026-05-27 22:00")
show(k1h, m, ["datetime", "open", "high", "low", "close", "volume",
              "taker_buy_base", "taker_sell_base", "delta", "buy_ratio", "cvd"],
     "1h orderflow — Trade2 上下文 (5/27 14:00 ~ 5/27 22:00)")

# 第二单 5m 全窗口：5/27 17:00 ~ 21:30
m = (k5m["datetime"] >= "2026-05-27 17:00") & (k5m["datetime"] < "2026-05-27 21:30")
show(k5m, m, ["datetime", "open", "high", "low", "close", "volume",
              "taker_buy_base", "taker_sell_base", "delta", "buy_ratio", "cvd"],
     "5m orderflow — Trade2 (5/27 17:00 ~ 21:30)")

# 关键 K 单行总结
print()
print("=" * 100)
print("关键 K 订单流")
print("=" * 100)
key_1h_times = ["2026-05-26 21:00", "2026-05-26 22:00", "2026-05-26 23:00",
                "2026-05-27 17:00", "2026-05-27 18:00", "2026-05-27 19:00",
                "2026-05-27 20:00", "2026-05-27 21:00"]
for t in key_1h_times:
    r = k1h[k1h["datetime"] == pd.Timestamp(t, tz="Asia/Shanghai")]
    if len(r):
        r = r.iloc[0]
        print(f"[1h] {t[5:]}  C={r.close:.3f}  vol={r.volume:>9.1f}  "
              f"buy={r.taker_buy_base:>8.1f}  sell={r.taker_sell_base:>8.1f}  "
              f"delta={r.delta:>+8.1f}  buy_ratio={r.buy_ratio:.3f}  cvd={r.cvd:>+10.1f}")

print()
key_5m_times = ["2026-05-27 19:55", "2026-05-27 20:00", "2026-05-27 20:05",
                "2026-05-27 20:10", "2026-05-27 20:15", "2026-05-27 20:20",
                "2026-05-27 20:25", "2026-05-27 20:30", "2026-05-27 20:35"]
for t in key_5m_times:
    r = k5m[k5m["datetime"] == pd.Timestamp(t, tz="Asia/Shanghai")]
    if len(r):
        r = r.iloc[0]
        print(f"[5m] {t[5:]}  C={r.close:.3f}  vol={r.volume:>9.1f}  "
              f"buy={r.taker_buy_base:>8.1f}  sell={r.taker_sell_base:>8.1f}  "
              f"delta={r.delta:>+8.1f}  buy_ratio={r.buy_ratio:.3f}  cvd={r.cvd:>+10.1f}")
