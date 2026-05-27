"""SOL 永续过去 7 天 4h/1h 上下文 + 当前 15m 全日结构，
寻找空单入场条件。"""
import pandas as pd
import ccxt
from pathlib import Path
import sys
sys.path.insert(0, "/Volumes/external/trade/交易复盘")
from tools.config import PROXY

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 40)
pd.set_option("display.max_rows", 200)

ex = ccxt.binance({"options": {"defaultType": "future"}, "proxies": PROXY})

def fetch(tf, since_str, limit):
    since = ex.parse8601(since_str)
    o = ex.fetch_ohlcv("SOL/USDT", tf, since=since, limit=limit)
    df = pd.DataFrame(o, columns=["ts","o","h","l","c","v"])
    df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    df["ema200"] = df["c"].ewm(span=200, adjust=False).mean()
    df["ema55"] = df["c"].ewm(span=55, adjust=False).mean()
    df["ema21"] = df["c"].ewm(span=21, adjust=False).mean()
    return df

# 过去 10 天 4h
df4h = fetch("4h", "2026-05-14 00:00:00", 80)
print("=== SOL 4h 过去 ~10 天 ===")
print(df4h[["dt","o","h","l","c","v","ema21","ema55","ema200"]].tail(60).to_string(index=False))

# 过去 4 天 1h, 检查 EMA200 触碰失败次数
df1h = fetch("1h", "2026-05-19 00:00:00", 200)
print("\n=== SOL 1h 过去 ~5 天 EMA200 触碰记录 ===")
df1h["touch_above"] = (df1h["h"] >= df1h["ema200"]) & (df1h["c"] < df1h["ema200"])
df1h["close_above"] = df1h["c"] >= df1h["ema200"]
df1h["close_below"] = df1h["c"] < df1h["ema200"]
print(df1h[["dt","o","h","l","c","ema200","touch_above","close_above"]].tail(80).to_string(index=False))

# 当前 15m 全日（至最新）
df15 = fetch("15m", "2026-05-23 18:00:00", 200)
print("\n=== SOL 15m 5/23 18:00 至今 ===")
print(df15[["dt","o","h","l","c","v","ema21","ema55","ema200"]].tail(80).to_string(index=False))

# 关键统计
print("\n=== EMA200 关系总结（4h，过去 10 天）===")
df4h["above"] = df4h["c"] > df4h["ema200"]
df4h["below"] = df4h["c"] < df4h["ema200"]
print(f"4h 收在 EMA200 上方: {df4h['above'].sum()} 根 / 收下方: {df4h['below'].sum()} 根")
print(f"4h 最高: {df4h['h'].max():.3f} 出现于 {df4h.loc[df4h['h'].idxmax(),'dt']}")
print(f"4h 最近 EMA200: {df4h.iloc[-1]['ema200']:.3f}, 最新 close: {df4h.iloc[-1]['c']:.3f}")

# 1h EMA200 失败拒绝段（high 触及 ema200 但 close 在下方）
fail_rejects = df1h[(df1h["h"] >= df1h["ema200"]) & (df1h["c"] < df1h["ema200"])]
print(f"\n1h EMA200 'high 触碰但 close 落回下方' 次数: {len(fail_rejects)}")
print(fail_rejects[["dt","h","c","ema200"]].to_string(index=False))
