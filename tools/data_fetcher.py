"""ccxt 数据获取封装：以锚定时间为中心，向前拉 warmup + display_before 根，向后拉 display_after 根。

设计要点
- 时区统一 UTC+8（Asia/Shanghai）。
- 返回 DataFrame 以 datetime(UTC+8) 为 index，列为 open/high/low/close/volume/timestamp_ms。
- 一次拉取足够长，避免反复分页；anchor 时间附近向后可能还未到（例如用户凌晨复盘前一日行情），此时只拉到最新 K 线。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import ccxt
import pandas as pd

from .config import PROXY

TZ_CN = ZoneInfo("Asia/Shanghai")


# 周期 -> 毫秒
_TF_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
}


@dataclass
class FetchSpec:
    exchange: str = "binance"           # binance / okx / bybit
    market: str = "futures"             # spot / futures
    base: str = "BTC"
    quote: str = "USDT"
    timeframe: str = "1h"
    anchor_cn: str = ""                 # UTC+8 时间字符串，如 "2026-04-18 14:00"
    bars_before: int = 780              # warmup + display_before
    bars_after: int = 20                # display_after

    @property
    def tf_ms(self) -> int:
        if self.timeframe not in _TF_MS:
            raise ValueError(f"unsupported timeframe: {self.timeframe}")
        return _TF_MS[self.timeframe]


def _build_exchange(exchange_id: str, market: str) -> ccxt.Exchange:
    klass = getattr(ccxt, exchange_id)
    options = {"defaultType": "future" if market == "futures" else "spot"}
    if exchange_id == "okx" and market == "futures":
        options = {"defaultType": "swap"}
    if exchange_id == "bybit" and market == "futures":
        options = {"defaultType": "linear"}
    ex = klass({
        "proxies": PROXY,
        "enableRateLimit": True,
        "options": options,
    })
    ex.load_markets()
    return ex


def _build_symbol(exchange_id: str, market: str, base: str, quote: str) -> str:
    # binance: BTC/USDT（合约也是该格式，配合 defaultType=future）
    # okx 合约: BTC/USDT:USDT
    # bybit 合约: BTC/USDT（配合 linear）
    if exchange_id == "okx" and market == "futures":
        return f"{base}/{quote}:{quote}"
    return f"{base}/{quote}"


def _anchor_to_ms(anchor_cn: str) -> int:
    """UTC+8 字符串 -> UTC 毫秒"""
    dt = pd.to_datetime(anchor_cn).tz_localize(TZ_CN)
    return int(dt.tz_convert("UTC").timestamp() * 1000)


def fetch_around_anchor(spec: FetchSpec) -> pd.DataFrame:
    """以 anchor 为中心拉取 K 线。"""
    ex = _build_exchange(spec.exchange, spec.market)
    symbol = _build_symbol(spec.exchange, spec.market, spec.base, spec.quote)

    anchor_ms = _anchor_to_ms(spec.anchor_cn)
    tf_ms = spec.tf_ms

    start_ms = anchor_ms - spec.bars_before * tf_ms
    end_ms = anchor_ms + (spec.bars_after + 1) * tf_ms  # +1 确保包含 anchor 后的最后一根

    all_rows: list[list] = []
    cursor = start_ms
    # ccxt 多数实现单次返回 1000 条
    batch_limit = 1000
    # 保险迭代上限
    safety = 0
    while cursor < end_ms and safety < 200:
        safety += 1
        rows = ex.fetch_ohlcv(symbol, timeframe=spec.timeframe, since=cursor, limit=batch_limit)
        if not rows:
            break
        all_rows.extend(rows)
        last_ts = rows[-1][0]
        if last_ts <= cursor:  # 防止原地踏步
            break
        cursor = last_ts + tf_ms
        if len(rows) < batch_limit:
            # 已到数据末端
            if cursor >= end_ms:
                break

    if not all_rows:
        raise RuntimeError(f"未获取到任何K线: {symbol} {spec.timeframe} anchor={spec.anchor_cn}")

    df = pd.DataFrame(all_rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    # 截窗：保留 [start_ms, end_ms]
    df = df[(df["timestamp"] >= start_ms) & (df["timestamp"] < end_ms)].reset_index(drop=True)

    # 时区转换为 UTC+8
    df["datetime"] = (
        pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(TZ_CN)
    )
    df = df[["datetime", "timestamp", "open", "high", "low", "close", "volume"]]
    df.attrs["symbol"] = symbol
    df.attrs["exchange"] = spec.exchange
    df.attrs["market"] = spec.market
    df.attrs["timeframe"] = spec.timeframe
    df.attrs["anchor_cn"] = spec.anchor_cn
    df.attrs["anchor_ms"] = anchor_ms
    return df


def find_anchor_index(df: pd.DataFrame, anchor_ms: int) -> int:
    """返回 anchor 所在 K 线的 iloc 位置（若 anchor 恰好在某根 K 线的开盘时间）。"""
    # anchor 在某根 K 线的 [open_time, open_time + tf) 区间内
    tf_ms = int(df["timestamp"].diff().median())
    mask = (df["timestamp"] <= anchor_ms) & (anchor_ms < df["timestamp"] + tf_ms)
    idx = df.index[mask]
    if len(idx) == 0:
        # 取最接近的
        return int((df["timestamp"] - anchor_ms).abs().idxmin())
    return int(idx[0])
