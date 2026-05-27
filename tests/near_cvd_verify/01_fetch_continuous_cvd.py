"""
步骤 1：拉取 NEAR/USDT 永续 + 现货的连续日 CVD
- 使用 Binance fapi/spot klines 直接 REST，保留 taker_buy_base_asset_volume 字段
- delta = 2 * taker_buy_base - volume   （即 taker_buy - taker_sell）
- 每根 1d K 线都有，不再抽样

输出：
  data/near_perp_daily_full.parquet
  data/near_spot_daily_full.parquet
"""
from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import requests

PROXY = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890",
}

OUT_DIR = Path(__file__).resolve().parent / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

START_MS = int(pd.Timestamp("2026-01-01", tz="UTC").timestamp() * 1000)
END_MS = int(pd.Timestamp("2026-05-23", tz="UTC").timestamp() * 1000)


def fetch_klines(base_url: str, symbol: str, interval: str = "1d") -> pd.DataFrame:
    """通用 REST 拉日线，返回带 taker_buy_base 的完整 DataFrame"""
    url = f"{base_url}/klines"
    rows = []
    cursor = START_MS
    while cursor < END_MS:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cursor,
            "endTime": END_MS,
            "limit": 1000,
        }
        r = requests.get(url, params=params, proxies=PROXY, timeout=20)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        last = batch[-1][0]
        if last <= cursor:
            break
        cursor = last + 1
        if len(batch) < 1000:
            break
        time.sleep(0.2)

    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades",
        "taker_buy_base", "taker_buy_quote", "ignore",
    ]
    df = pd.DataFrame(rows, columns=cols)
    for c in ["open", "high", "low", "close", "volume",
              "quote_volume", "taker_buy_base", "taker_buy_quote"]:
        df[c] = df[c].astype(float)
    df["trades"] = df["trades"].astype(int)
    df["datetime_utc"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["datetime_cn"] = df["datetime_utc"].dt.tz_convert("Asia/Shanghai")
    df["date_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d")

    # 派生订单流字段
    df["taker_sell_base"] = df["volume"] - df["taker_buy_base"]
    df["delta"] = df["taker_buy_base"] - df["taker_sell_base"]   # 净 taker buy
    df["buy_ratio"] = df["taker_buy_base"] / df["volume"]
    df["cvd"] = df["delta"].cumsum()
    return df


def main():
    print("[1/2] 拉取永续 NEARUSDT 日线 ...")
    perp = fetch_klines("https://fapi.binance.com/fapi/v1", "NEARUSDT")
    print(f"  perp rows={len(perp)}  range={perp['date_utc'].iloc[0]} .. {perp['date_utc'].iloc[-1]}")
    perp.to_parquet(OUT_DIR / "near_perp_daily_full.parquet", index=False)
    perp.to_csv(OUT_DIR / "near_perp_daily_full.csv", index=False)

    print("[2/2] 拉取现货 NEARUSDT 日线 ...")
    spot = fetch_klines("https://api.binance.com/api/v3", "NEARUSDT")
    print(f"  spot rows={len(spot)}  range={spot['date_utc'].iloc[0]} .. {spot['date_utc'].iloc[-1]}")
    spot.to_parquet(OUT_DIR / "near_spot_daily_full.parquet", index=False)
    spot.to_csv(OUT_DIR / "near_spot_daily_full.csv", index=False)

    # 简要对比
    print("\n=== 对比关键日期 ===")
    keys = ["2026-02-23", "2026-02-24", "2026-02-25", "2026-02-26", "2026-03-05",
            "2026-04-09", "2026-04-23", "2026-05-17", "2026-05-22"]
    for d in keys:
        pr = perp[perp["date_utc"] == d]
        sp = spot[spot["date_utc"] == d]
        if pr.empty or sp.empty:
            continue
        pr = pr.iloc[0]; sp = sp.iloc[0]
        print(f"{d} | perp close={pr['close']:.3f} delta={pr['delta']:>+13.0f} buy%={pr['buy_ratio']*100:.1f} "
              f"| spot close={sp['close']:.3f} delta={sp['delta']:>+13.0f} buy%={sp['buy_ratio']*100:.1f}")


if __name__ == "__main__":
    main()
