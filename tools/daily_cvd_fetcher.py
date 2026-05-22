"""日级 CVD 抓取工具（永续 + 现货 + funding + OI + topLSR）

设计原则
========
- **数据源铁律**：日级 CVD 一律用 Binance klines 自带的 `taker_buy_base_asset_volume`
  推算 delta，禁止用 aggTrades 做日级累计（fromId 翻页有跨日 bug）
- **双端默认**：永续 + 现货同时拉，缺一不可下结论
- **可选附属**：funding rate（全历史）、OI 历史（近30天）、top trader 多空比（近30天）

使用
====

作为模块::

    from tools.daily_cvd_fetcher import fetch_daily_cvd
    out = fetch_daily_cvd(base="NEAR", start="2026-01-01",
                          markets=("perp", "spot"),
                          with_funding=True, with_oi=True, with_lsr=True)
    perp_df, spot_df = out["perp"], out["spot"]
    funding_df = out.get("funding")

CLI::

    python -m tools.daily_cvd_fetcher --base NEAR --start 2026-01-01 \\
        --markets perp,spot --funding --oi --lsr \\
        --out-dir reviews/<topic>/data

CLI 会把所有 DataFrame 同时存成 .parquet 和 .csv，并打印简要 summary。
"""
from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests

from .config import PROXY


# ----------- HTTP helpers -----------

def _get(url: str, params: dict, timeout: int = 20) -> list | dict:
    r = requests.get(url, params=params, proxies=PROXY, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _to_ms(ts: str | int | pd.Timestamp) -> int:
    if isinstance(ts, int):
        return ts
    return int(pd.Timestamp(ts, tz="UTC").timestamp() * 1000)


# ----------- klines (perp / spot) -----------

_KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades",
    "taker_buy_base", "taker_buy_quote", "ignore",
]


def _fetch_klines(base_url: str, symbol: str, start_ms: int, end_ms: int,
                  interval: str = "1d") -> pd.DataFrame:
    rows: list = []
    cursor = start_ms
    while cursor < end_ms:
        batch = _get(f"{base_url}/klines", {
            "symbol": symbol, "interval": interval,
            "startTime": cursor, "endTime": end_ms, "limit": 1000,
        })
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
    df = pd.DataFrame(rows, columns=_KLINE_COLS)
    if df.empty:
        return df
    for c in ["open", "high", "low", "close", "volume",
              "quote_volume", "taker_buy_base", "taker_buy_quote"]:
        df[c] = df[c].astype(float)
    df["trades"] = df["trades"].astype(int)
    df["datetime_utc"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["datetime_cn"] = df["datetime_utc"].dt.tz_convert("Asia/Shanghai")
    df["date_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d")
    df["taker_sell_base"] = df["volume"] - df["taker_buy_base"]
    df["delta"] = df["taker_buy_base"] - df["taker_sell_base"]
    df["buy_ratio"] = df["taker_buy_base"] / df["volume"].where(df["volume"] > 0, 1)
    df["cvd"] = df["delta"].cumsum()
    return df


# ----------- funding / OI / LSR -----------

def _fetch_funding(symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    rows: list = []
    cursor = start_ms
    while cursor < end_ms:
        batch = _get("https://fapi.binance.com/fapi/v1/fundingRate", {
            "symbol": symbol, "startTime": cursor,
            "endTime": end_ms, "limit": 1000,
        })
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
    if df.empty:
        return df
    df["fundingRate"] = df["fundingRate"].astype(float)
    df["datetime_utc"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
    df["datetime_cn"] = df["datetime_utc"].dt.tz_convert("Asia/Shanghai")
    df["date_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d")
    return df


def _fetch_oi(symbol: str, end_ms: int, period: str = "1d") -> pd.DataFrame:
    """OI history 仅近 30 天，自动从 end-29d 起拉（留 1 天 buffer 防边界拒绝）"""
    start_ms = end_ms - 29 * 24 * 3600 * 1000
    rows: list = []
    cursor = start_ms
    while cursor < end_ms:
        batch = _get("https://fapi.binance.com/futures/data/openInterestHist", {
            "symbol": symbol, "period": period,
            "startTime": cursor, "endTime": end_ms, "limit": 500,
        })
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
    if df.empty:
        return df
    for c in ("sumOpenInterest", "sumOpenInterestValue"):
        df[c] = df[c].astype(float)
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["date_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d")
    return df


def _fetch_lsr(symbol: str, end_ms: int, period: str = "1d") -> pd.DataFrame:
    """top trader 多空账户比 仅近 30 天（留 1 天 buffer）"""
    start_ms = end_ms - 29 * 24 * 3600 * 1000
    rows: list = []
    cursor = start_ms
    while cursor < end_ms:
        batch = _get("https://fapi.binance.com/futures/data/topLongShortAccountRatio", {
            "symbol": symbol, "period": period,
            "startTime": cursor, "endTime": end_ms, "limit": 500,
        })
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
    if df.empty:
        return df
    for c in ("longShortRatio", "longAccount", "shortAccount"):
        df[c] = df[c].astype(float)
    df["datetime_utc"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["date_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d")
    return df


# ----------- 顶层 API -----------

@dataclass
class FetchResult:
    """轻封装。提供 dict 风格访问。"""
    perp: Optional[pd.DataFrame] = None
    spot: Optional[pd.DataFrame] = None
    funding: Optional[pd.DataFrame] = None
    oi_1d: Optional[pd.DataFrame] = None
    lsr: Optional[pd.DataFrame] = None

    def __getitem__(self, k: str) -> Optional[pd.DataFrame]:
        return getattr(self, k)

    def get(self, k: str, default=None):
        return getattr(self, k, default)


def fetch_daily_cvd(
    base: str,
    quote: str = "USDT",
    start: str = "2026-01-01",
    end: Optional[str] = None,
    markets: Iterable[str] = ("perp", "spot"),
    interval: str = "1d",
    with_funding: bool = False,
    with_oi: bool = False,
    with_lsr: bool = False,
) -> FetchResult:
    """拉取多日 CVD 全量数据。

    Args:
        base/quote: 例如 NEAR / USDT
        start: UTC 日期或时间字符串
        end: 同上，默认到现在
        markets: ("perp",) / ("spot",) / ("perp","spot")
        interval: 默认 1d；也可 4h/1h
        with_funding/with_oi/with_lsr: 附属指标开关
    """
    symbol = f"{base.upper()}{quote.upper()}"
    start_ms = _to_ms(start)
    end_ms = _to_ms(end) if end else int(pd.Timestamp.now(tz="UTC").timestamp() * 1000)

    res = FetchResult()
    if "perp" in markets:
        res.perp = _fetch_klines(
            "https://fapi.binance.com/fapi/v1", symbol, start_ms, end_ms, interval)
    if "spot" in markets:
        res.spot = _fetch_klines(
            "https://api.binance.com/api/v3", symbol, start_ms, end_ms, interval)
    if with_funding:
        res.funding = _fetch_funding(symbol, start_ms, end_ms)
    if with_oi:
        res.oi_1d = _fetch_oi(symbol, end_ms, period="1d")
    if with_lsr:
        res.lsr = _fetch_lsr(symbol, end_ms, period="1d")
    return res


def cross_check(klines_df: pd.DataFrame,
                aggtrades_total: float,
                tol: float = 0.05) -> dict:
    """数据核验：klines volume 总和 vs aggTrades qty 总和，偏差 > tol 即报警

    用于在用 aggTrades 做日内/分钟级订单流时双向确认数据完整性。
    """
    klines_total = float(klines_df["volume"].sum())
    diff = abs(klines_total - aggtrades_total) / klines_total if klines_total else 1.0
    return {
        "klines_total": klines_total,
        "aggtrades_total": aggtrades_total,
        "deviation": diff,
        "ok": diff <= tol,
        "tol": tol,
    }


# ----------- CLI -----------

def _save(df: Optional[pd.DataFrame], out_dir: Path, name: str) -> None:
    if df is None or df.empty:
        print(f"  [skip] {name}: empty")
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / f"{name}.parquet", index=False)
    df.to_csv(out_dir / f"{name}.csv", index=False)
    print(f"  [save] {name}: rows={len(df)} → {out_dir}/{name}.{{parquet,csv}}")


def _summary(res: FetchResult) -> None:
    def _phase_buy_ratio(df: pd.DataFrame) -> float:
        v = df["volume"].sum()
        return df["taker_buy_base"].sum() / v if v else 0

    print("\n=== summary ===")
    for k in ("perp", "spot"):
        df = res[k]
        if df is None or df.empty:
            continue
        print(f"[{k}] rows={len(df)} {df['date_utc'].iloc[0]}..{df['date_utc'].iloc[-1]} "
              f"close {df['close'].iloc[0]:.4f}→{df['close'].iloc[-1]:.4f} "
              f"({(df['close'].iloc[-1]/df['close'].iloc[0]-1)*100:+.1f}%) "
              f"ΣΔ={df['delta'].sum():+,.0f} buy%={_phase_buy_ratio(df)*100:.2f}")
    if res.funding is not None and not res.funding.empty:
        f = res.funding
        print(f"[funding] n={len(f)} mean_bps={f['fundingRate'].mean()*1e4:+.3f} "
              f"sum%={f['fundingRate'].sum()*100:+.3f}")
    if res.oi_1d is not None and not res.oi_1d.empty:
        oi = res.oi_1d
        print(f"[oi_1d] n={len(oi)} {oi['date_utc'].iloc[0]}..{oi['date_utc'].iloc[-1]} "
              f"OI {oi['sumOpenInterest'].iloc[0]/1e6:.2f}M→{oi['sumOpenInterest'].iloc[-1]/1e6:.2f}M")
    if res.lsr is not None and not res.lsr.empty:
        l = res.lsr
        print(f"[topLSR] n={len(l)} ratio range {l['longShortRatio'].min():.2f}..{l['longShortRatio'].max():.2f} "
              f"latest={l['longShortRatio'].iloc[-1]:.2f}")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Binance 日级 CVD 抓取（永续+现货）+ funding/OI/topLSR")
    p.add_argument("--base", required=True, help="如 NEAR")
    p.add_argument("--quote", default="USDT")
    p.add_argument("--start", required=True, help="如 2026-01-01")
    p.add_argument("--end", default=None, help="默认到现在")
    p.add_argument("--markets", default="perp,spot",
                   help="逗号分隔，可选 perp / spot")
    p.add_argument("--interval", default="1d")
    p.add_argument("--funding", action="store_true")
    p.add_argument("--oi", action="store_true")
    p.add_argument("--lsr", action="store_true")
    p.add_argument("--out-dir", default=None,
                   help="数据保存目录；不传则只打印 summary")
    args = p.parse_args()

    markets = tuple(m.strip() for m in args.markets.split(",") if m.strip())
    print(f"[fetch] {args.base}/{args.quote} {args.start}..{args.end or 'now'} "
          f"markets={markets} interval={args.interval} "
          f"funding={args.funding} oi={args.oi} lsr={args.lsr}")
    res = fetch_daily_cvd(
        base=args.base, quote=args.quote,
        start=args.start, end=args.end,
        markets=markets, interval=args.interval,
        with_funding=args.funding, with_oi=args.oi, with_lsr=args.lsr,
    )

    if args.out_dir:
        out = Path(args.out_dir)
        sym = f"{args.base.lower()}_{args.quote.lower()}"
        _save(res.perp, out, f"{sym}_perp_daily")
        _save(res.spot, out, f"{sym}_spot_daily")
        _save(res.funding, out, f"{sym}_funding")
        _save(res.oi_1d, out, f"{sym}_oi_1d")
        _save(res.lsr, out, f"{sym}_top_lsr")

    _summary(res)


if __name__ == "__main__":
    main()
