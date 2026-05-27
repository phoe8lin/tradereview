"""
步骤 2：拉取 NEAR 永续 funding rate + OI 历史
- funding rate: fapi/v1/fundingRate (8小时一次, 全历史可拉)
- OI:           futures/data/openInterestHist  (period=1d, 仅近 30 天)
- top trader long/short ratio: futures/data/topLongShortAccountRatio
"""
from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import requests

PROXY = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}
OUT = Path(__file__).resolve().parent / "data"
OUT.mkdir(parents=True, exist_ok=True)

START_MS = int(pd.Timestamp("2026-01-01", tz="UTC").timestamp() * 1000)
END_MS = int(pd.Timestamp("2026-05-23", tz="UTC").timestamp() * 1000)
SYMBOL = "NEARUSDT"


def get(url, params):
    r = requests.get(url, params=params, proxies=PROXY, timeout=20)
    r.raise_for_status()
    return r.json()


def fetch_funding():
    rows = []
    cursor = START_MS
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    while cursor < END_MS:
        batch = get(url, {"symbol": SYMBOL, "startTime": cursor,
                          "endTime": END_MS, "limit": 1000})
        if not batch:
            break
        rows.extend(batch)
        last = batch[-1]["fundingTime"]
        if last <= cursor:
            break
        cursor = last + 1
        if len(batch) < 1000:
            break
        time.sleep(0.2)
    df = pd.DataFrame(rows)
    df["fundingRate"] = df["fundingRate"].astype(float)
    df["datetime_utc"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
    df["datetime_cn"] = df["datetime_utc"].dt.tz_convert("Asia/Shanghai")
    df["date_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d")
    return df


def fetch_oi():
    """OI history. period=1d, Binance 仅返回最近 30 天。也试 period=4h 看看能否更长。"""
    out = {}
    # 只能拿到最近 30 天，所以 startTime 设为 30 天前
    oi_start = END_MS - 30 * 24 * 3600 * 1000
    for period in ["1d", "4h"]:
        rows = []
        cursor = oi_start
        url = "https://fapi.binance.com/futures/data/openInterestHist"
        while cursor < END_MS:
            batch = get(url, {"symbol": SYMBOL, "period": period,
                              "startTime": cursor, "endTime": END_MS, "limit": 500})
            if not batch:
                break
            rows.extend(batch)
            last = batch[-1]["timestamp"]
            if last <= cursor:
                break
            cursor = last + 1
            if len(batch) < 500:
                break
            time.sleep(0.2)
        df = pd.DataFrame(rows)
        if not df.empty:
            df["sumOpenInterest"] = df["sumOpenInterest"].astype(float)
            df["sumOpenInterestValue"] = df["sumOpenInterestValue"].astype(float)
            df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            df["date_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d")
        out[period] = df
    return out


def fetch_lsr():
    """top trader 多空账户比 (1d, 30 days max)"""
    rows = []
    url = "https://fapi.binance.com/futures/data/topLongShortAccountRatio"
    cursor = END_MS - 30 * 24 * 3600 * 1000
    while cursor < END_MS:
        batch = get(url, {"symbol": SYMBOL, "period": "1d",
                          "startTime": cursor, "endTime": END_MS, "limit": 500})
        if not batch:
            break
        rows.extend(batch)
        last = batch[-1]["timestamp"]
        if last <= cursor:
            break
        cursor = last + 1
        if len(batch) < 500:
            break
        time.sleep(0.2)
    df = pd.DataFrame(rows)
    if not df.empty:
        for c in ["longShortRatio", "longAccount", "shortAccount"]:
            df[c] = df[c].astype(float)
        df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df["date_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d")
    return df


def main():
    print("[1/3] funding rate ...")
    fr = fetch_funding()
    print(f"  rows={len(fr)}, range={fr['date_utc'].min()} .. {fr['date_utc'].max()}")
    fr.to_parquet(OUT / "near_funding.parquet", index=False)
    fr.to_csv(OUT / "near_funding.csv", index=False)

    # 日聚合
    fr_daily = fr.groupby("date_utc")["fundingRate"].agg(["mean", "sum", "min", "max", "count"]).reset_index()
    fr_daily.columns = ["date_utc", "fr_mean", "fr_sum", "fr_min", "fr_max", "fr_count"]
    fr_daily.to_csv(OUT / "near_funding_daily.csv", index=False)
    print(f"  daily mean range: {fr_daily['fr_mean'].min():.6f} .. {fr_daily['fr_mean'].max():.6f}")

    print("[2/3] OI history ...")
    oi = fetch_oi()
    for p, df in oi.items():
        if df.empty:
            print(f"  period={p}: empty")
        else:
            print(f"  period={p}: rows={len(df)}, "
                  f"range={df['date_utc'].min()} .. {df['date_utc'].max()}")
            df.to_parquet(OUT / f"near_oi_{p}.parquet", index=False)
            df.to_csv(OUT / f"near_oi_{p}.csv", index=False)

    print("[3/3] top trader long/short ratio ...")
    lsr = fetch_lsr()
    if lsr.empty:
        print("  empty")
    else:
        print(f"  rows={len(lsr)}, range={lsr['date_utc'].min()} .. {lsr['date_utc'].max()}")
        lsr.to_parquet(OUT / "near_top_lsr.parquet", index=False)
        lsr.to_csv(OUT / "near_top_lsr.csv", index=False)


if __name__ == "__main__":
    main()
