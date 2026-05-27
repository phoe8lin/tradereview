"""核验用户方法论：
1. 大窗口（4h）均线是否交缠？
2. 小窗口（5m）20:20 前后是否经历"多头排列下价格跌破 ema21 → 反弹"？
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import pandas as pd

from tools.data_fetcher import FetchSpec, fetch_around_anchor
from tools.indicators import add_ema, classify_ema_stack
from tools.kline_features import add_kline_features

pd.set_option("display.width", 240)
pd.set_option("display.max_columns", 30)
pd.set_option("display.float_format", lambda x: f"{x:.3f}")

# ---------- 4h ----------
spec_4h = FetchSpec(
    exchange="binance", market="futures", base="HYPE", quote="USDT",
    timeframe="4h", anchor_cn="2026-05-27 20:00",
    bars_before=200, bars_after=12,
)
df_4h = fetch_around_anchor(spec_4h)
add_ema(df_4h, periods=(21, 55, 100, 200))
add_kline_features(df_4h)
df_4h["ema_stack"] = df_4h.apply(classify_ema_stack, axis=1)

mask = (df_4h["datetime"] >= "2026-05-22 00:00") & (df_4h["datetime"] < "2026-05-28 04:00")
print("=" * 100)
print("HYPE 4h ema_stack 序列（5/22 - 5/27）")
print("=" * 100)
sub = df_4h.loc[mask].copy()
sub["datetime"] = sub["datetime"].dt.strftime("%m-%d %H:%M")
print(sub[["datetime", "close", "ema_21", "ema_55", "ema_100", "ema_200", "ema_stack"]].to_string(index=False))

# ---------- 1h ----------
spec_1h = FetchSpec(
    exchange="binance", market="futures", base="HYPE", quote="USDT",
    timeframe="1h", anchor_cn="2026-05-27 20:00",
    bars_before=300, bars_after=12,
)
df_1h = fetch_around_anchor(spec_1h)
add_ema(df_1h, periods=(21, 55, 100, 200))
df_1h["ema_stack"] = df_1h.apply(classify_ema_stack, axis=1)

print()
print("=" * 100)
print("HYPE 1h ema_stack 序列（5/26 12:00 - 5/27 22:00）")
print("=" * 100)
mask = (df_1h["datetime"] >= "2026-05-26 12:00") & (df_1h["datetime"] < "2026-05-27 23:00")
sub = df_1h.loc[mask].copy()
sub["datetime"] = sub["datetime"].dt.strftime("%m-%d %H:%M")
# 21-55-100 紧凑度（max-min 占 close 比例）
sub["ema_spread_pct"] = (sub[["ema_21", "ema_55", "ema_100"]].max(axis=1)
                        - sub[["ema_21", "ema_55", "ema_100"]].min(axis=1)) / sub["close"] * 100
print(sub[["datetime", "close", "ema_21", "ema_55", "ema_100", "ema_200", "ema_stack", "ema_spread_pct"]].to_string(index=False))

# ---------- 5m around 20:20 ----------
spec_5m = FetchSpec(
    exchange="binance", market="futures", base="HYPE", quote="USDT",
    timeframe="5m", anchor_cn="2026-05-27 20:20",
    bars_before=200, bars_after=30,
)
df_5m = fetch_around_anchor(spec_5m)
add_ema(df_5m, periods=(21, 55, 100, 200))
df_5m["ema_stack"] = df_5m.apply(classify_ema_stack, axis=1)
df_5m["close_vs_ema21"] = df_5m["close"] - df_5m["ema_21"]
df_5m["price_breakdown"] = df_5m["close"] < df_5m["ema_21"]

print()
print("=" * 100)
print("HYPE 5m 价格 vs EMA21（5/27 17:00 - 20:35）")
print("=" * 100)
mask = (df_5m["datetime"] >= "2026-05-27 17:00") & (df_5m["datetime"] < "2026-05-27 20:40")
sub = df_5m.loc[mask].copy()
sub["datetime"] = sub["datetime"].dt.strftime("%H:%M")
print(sub[["datetime", "close", "ema_21", "close_vs_ema21", "ema_55", "ema_200",
           "ema_stack", "price_breakdown"]].to_string(index=False))

# 关键节点：5m 第一次跌破 ema21 的时间
first_break = df_5m[(df_5m["datetime"] >= "2026-05-27 19:00") & (df_5m["price_breakdown"])].head(3)
print()
print("[5m] 19:00 之后第一次跌破 ema21 的 3 根：")
fb = first_break.copy()
fb["datetime"] = fb["datetime"].dt.strftime("%H:%M")
print(fb[["datetime", "open", "high", "low", "close", "ema_21", "close_vs_ema21"]].to_string(index=False))
