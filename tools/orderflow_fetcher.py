"""按需拉取订单流：klines bar-level delta/cvd + 按钩子拉热点 aggTrades。

设计要点（v2，2026-05 重构）
============================
- **bar-level delta/cvd 永远来自 Binance klines.taker_buy_base_asset_volume**
  （便宜、零分页 bug、零时区错位）
- **aggTrades 只在 hybrid/full 模式下拉**，用于保留 footprint / 大单等微观证据
  - ``klines`` 模式：完全不拉 aggTrades；最快
  - ``hybrid``（**默认**）：仅热点 bar（按钩子识别 + buffer）拉 aggTrades
  - ``full``：整个窗口拉 aggTrades（保留旧行为）
- 钩子见 ``tools.orderflow_hooks``：anchor / long_wick / absorption_candidate /
  engulf / wave_extreme / high_volume / 显式 bar_ids。

产物
====
- ``<id>.orderflow.parquet`` — 全窗口 K 线 + delta/cvd（始终来自 klines）
- ``<id>.trades.parquet`` — aggTrades 原始数据（hybrid: 仅热点段；full: 整窗口；klines: 不写）
- ``<id>.hotspots.json`` — 热点命中记录（hybrid/full 写；klines: 不写）

向后兼容
========
- 不传 ``mode`` 时使用 ``config.orderflow.default_mode``（默认 hybrid）
- 旧调用 ``build_orderflow(date, trade_id)`` 仍能工作

命令行
======

::

    # hybrid 模式（默认） — 推荐
    python -m tools.orderflow_fetcher --date 2026-04-20 --trade-id HYPE_5m_001

    # 仅 klines（最快、无 footprint）
    python -m tools.orderflow_fetcher --date 2026-04-20 --trade-id HYPE_5m_001 --mode klines

    # 整窗口 aggTrades（与旧版 v1 行为一致）
    python -m tools.orderflow_fetcher --date 2026-04-20 --trade-id HYPE_5m_001 --mode full

    # 自定义钩子
    python -m tools.orderflow_fetcher --date 2026-04-20 --trade-id HYPE_5m_001 \
        --hooks anchor,long_upper_wick,absorption_candidate --buffer-bars 2
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import List, Optional

import ccxt
import pandas as pd
import requests
import yaml

from .config import PROJECT_ROOT, PROXY, load_defaults
from .data_fetcher import _build_exchange, _build_symbol, _TF_MS, TZ_CN
from .orderflow_hooks import detect_hotspots


_KLINE_COLS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades_count",
    "taker_buy_base", "taker_buy_quote", "ignore",
]


# ----------------- 元数据 -----------------

def _load_trade_meta(review_date: str, trade_id: str) -> dict:
    yaml_path = PROJECT_ROOT / "reviews" / review_date / "trades" / f"{trade_id}.yaml"
    if not yaml_path.exists():
        raise FileNotFoundError(f"找不到 trade yaml: {yaml_path}")
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _anchor_ms_from_cn(anchor_cn: str) -> int:
    dt = pd.to_datetime(anchor_cn).tz_localize(TZ_CN)
    return int(dt.tz_convert("UTC").timestamp() * 1000)


def _binance_market_url(market: str) -> str:
    if market == "futures":
        return "https://fapi.binance.com/fapi/v1"
    return "https://api.binance.com/api/v3"


# ----------------- klines (with taker_buy_base) -----------------

def fetch_klines_with_taker(
    market: str,
    base: str,
    quote: str,
    timeframe: str,
    start_ms: int,
    end_ms: int,
) -> pd.DataFrame:
    """直接调 Binance /klines（绕过 ccxt 以拿到 taker_buy_base 字段）。"""
    base_url = _binance_market_url(market)
    symbol = f"{base.upper()}{quote.upper()}"
    rows: list = []
    cursor = start_ms
    while cursor < end_ms:
        r = requests.get(f"{base_url}/klines", params={
            "symbol": symbol, "interval": timeframe,
            "startTime": cursor, "endTime": end_ms, "limit": 1000,
        }, proxies=PROXY, timeout=20)
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
        time.sleep(0.15)

    df = pd.DataFrame(rows, columns=_KLINE_COLS)
    if df.empty:
        return df
    for c in ("open", "high", "low", "close", "volume",
              "quote_volume", "taker_buy_base", "taker_buy_quote"):
        df[c] = df[c].astype(float)
    df["trades_count"] = df["trades_count"].astype(int)
    df["timestamp"] = df["open_time"].astype(int)
    df["taker_sell_base"] = df["volume"] - df["taker_buy_base"]
    df["delta"] = df["taker_buy_base"] - df["taker_sell_base"]
    df["buy_ratio"] = df["taker_buy_base"] / df["volume"].where(df["volume"] > 0, 1)
    return df[[
        "timestamp", "open", "high", "low", "close", "volume",
        "taker_buy_base", "taker_sell_base", "delta", "buy_ratio", "trades_count",
    ]]


# ----------------- aggTrades -----------------

def fetch_trades_window(
    exchange_id: str,
    market: str,
    base: str,
    quote: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> pd.DataFrame:
    """拉取 [start_ms, end_ms) 区间内的 aggTrades。"""
    ex = _build_exchange(exchange_id, market)
    symbol = _build_symbol(exchange_id, market, base, quote)

    all_rows: list[dict] = []
    cursor = start_ms
    safety = 0
    while cursor < end_ms and safety < 500:
        safety += 1
        try:
            rows = ex.fetch_trades(symbol, since=cursor, limit=limit)
        except ccxt.BaseError as e:
            print(f"[warn] fetch_trades 失败，退避 1s: {e}")
            time.sleep(1.0)
            continue
        if not rows:
            break
        all_rows.extend(rows)
        last_ts = rows[-1]["timestamp"]
        if last_ts <= cursor:
            break
        cursor = last_ts + 1
        if len(rows) < limit and cursor >= end_ms:
            break

    if not all_rows:
        return pd.DataFrame(columns=["timestamp", "price", "amount", "side"])

    df = pd.DataFrame([
        {
            "timestamp": r["timestamp"],
            "price": float(r["price"]),
            "amount": float(r["amount"]),
            "side": r["side"],
        }
        for r in all_rows if r.get("timestamp") is not None
    ])
    df = (df.drop_duplicates(subset=["timestamp", "price", "amount", "side"])
            .sort_values("timestamp")
            .reset_index(drop=True))
    df = df[(df["timestamp"] >= start_ms) & (df["timestamp"] < end_ms)].reset_index(drop=True)
    return df


def fetch_trades_for_ranges(
    exchange_id: str,
    market: str,
    base: str,
    quote: str,
    ranges: List[dict],
    limit: int = 1000,
) -> pd.DataFrame:
    """对一组连续时间段分别拉 aggTrades 并合并。"""
    parts: List[pd.DataFrame] = []
    for rg in ranges:
        df = fetch_trades_window(
            exchange_id, market, base, quote,
            int(rg["start_ts"]), int(rg["end_ts"]), limit=limit,
        )
        if not df.empty:
            parts.append(df)
    if not parts:
        return pd.DataFrame(columns=["timestamp", "price", "amount", "side"])
    out = pd.concat(parts, ignore_index=True)
    out = out.drop_duplicates(subset=["timestamp", "price", "amount", "side"]).sort_values("timestamp").reset_index(drop=True)
    return out


# ----------------- 订单流计算 -----------------

def compute_orderflow_from_klines(
    klines_local: pd.DataFrame,
    klines_taker: pd.DataFrame,
) -> pd.DataFrame:
    """从 klines.taker_buy_base 推算每根 K 的 delta/cvd（窗口内累积）。

    Args:
        klines_local: review_builder 产生的 <id>.parquet（含 kline_id / datetime / close）
        klines_taker: fetch_klines_with_taker 返回的 raw klines
    """
    base = klines_local[["kline_id", "datetime", "timestamp", "close"]].copy()
    merge = base.merge(
        klines_taker[["timestamp", "taker_buy_base", "taker_sell_base", "delta", "buy_ratio", "volume"]],
        on="timestamp", how="left",
    )
    merge = merge.rename(columns={
        "taker_buy_base": "buy_vol",
        "taker_sell_base": "sell_vol",
        "volume": "total_trades_vol",
    })
    merge[["buy_vol", "sell_vol", "total_trades_vol", "delta"]] = (
        merge[["buy_vol", "sell_vol", "total_trades_vol", "delta"]].fillna(0.0)
    )
    merge["cvd"] = merge["delta"].cumsum()
    return merge[[
        "kline_id", "datetime", "timestamp", "close",
        "buy_vol", "sell_vol", "total_trades_vol", "delta", "cvd", "buy_ratio",
    ]]


def compute_orderflow_from_trades(klines_local: pd.DataFrame, trades: pd.DataFrame, tf_ms: int) -> pd.DataFrame:
    """旧版逻辑：按 K 线时间分桶 aggTrades 算 delta/cvd（仅 full 模式使用）。"""
    if trades.empty:
        of = klines_local[["kline_id", "datetime", "timestamp", "close"]].copy()
        for col in ["buy_vol", "sell_vol", "total_trades_vol", "delta", "cvd", "buy_ratio"]:
            of[col] = 0.0
        return of

    trades = trades.copy()
    trades["bar_ts"] = (trades["timestamp"] // tf_ms) * tf_ms
    trades["buy_vol"] = trades.apply(lambda r: r["amount"] if r["side"] == "buy" else 0.0, axis=1)
    trades["sell_vol"] = trades.apply(lambda r: r["amount"] if r["side"] == "sell" else 0.0, axis=1)
    grp = trades.groupby("bar_ts").agg(
        buy_vol=("buy_vol", "sum"),
        sell_vol=("sell_vol", "sum"),
    ).reset_index()
    grp["total_trades_vol"] = grp["buy_vol"] + grp["sell_vol"]
    grp["delta"] = grp["buy_vol"] - grp["sell_vol"]
    grp = grp.rename(columns={"bar_ts": "timestamp"})
    of = klines_local[["kline_id", "datetime", "timestamp", "close"]].merge(grp, on="timestamp", how="left")
    of[["buy_vol", "sell_vol", "total_trades_vol", "delta"]] = (
        of[["buy_vol", "sell_vol", "total_trades_vol", "delta"]].fillna(0.0)
    )
    of["cvd"] = of["delta"].cumsum()
    of["buy_ratio"] = of.apply(
        lambda r: (r["buy_vol"] / r["total_trades_vol"]) if r["total_trades_vol"] > 0 else float("nan"),
        axis=1,
    )
    return of


# ----------------- 主入口 -----------------

def build_orderflow(
    review_date: str,
    trade_id: str,
    window: Optional[int] = None,
    *,
    mode: Optional[str] = None,
    hooks: Optional[List[str]] = None,
    buffer_bars: Optional[int] = None,
    extra_bar_ids: Optional[List[str]] = None,
    force_refetch: bool = False,
) -> dict:
    """主入口：klines（默认） / hybrid / full 三种模式。

    Args:
        mode: ``klines`` / ``hybrid`` / ``full``，默认读 config
        hooks: 要启用的钩子列表，默认读 config
        buffer_bars: 热点 bar ±N 根 buffer，默认读 config
        extra_bar_ids: 显式追加的 bar_id（如锚 K 之外想强制拉的 bar）
        force_refetch: 忽略缓存重拉
    """
    cfg = load_defaults()
    of_cfg = cfg["orderflow"]
    if window is None:
        window = of_cfg["default_window"]
    if mode is None:
        mode = of_cfg.get("default_mode", "hybrid")
    if hooks is None:
        hooks = list(of_cfg.get("default_hooks", ["anchor"]))
    if buffer_bars is None:
        buffer_bars = int(of_cfg.get("default_buffer_bars", 1))
    thresholds = of_cfg.get("hook_thresholds", {})
    limit = of_cfg["trades_limit"]

    if mode not in ("klines", "hybrid", "full"):
        raise ValueError(f"未知 mode: {mode}（应为 klines/hybrid/full）")

    meta = _load_trade_meta(review_date, trade_id)
    exchange = meta["exchange"]
    market = meta["market"]
    symbol = meta["symbol"]
    timeframe = meta["timeframe"]
    anchor_cn = meta["anchor_cn"]
    base, quote = symbol.split(":")[0].split("/")

    if timeframe not in _TF_MS:
        raise ValueError(f"不支持的周期: {timeframe}")
    tf_ms = _TF_MS[timeframe]

    anchor_ms = _anchor_ms_from_cn(anchor_cn)
    start_ms = anchor_ms - window * tf_ms
    end_ms = anchor_ms + (window + 1) * tf_ms

    data_dir = PROJECT_ROOT / "reviews" / review_date / "data"
    kline_path = data_dir / f"{trade_id}.parquet"
    trades_path = data_dir / f"{trade_id}.trades.parquet"
    of_path = data_dir / f"{trade_id}.orderflow.parquet"
    hotspots_path = data_dir / f"{trade_id}.hotspots.json"

    klines_local = pd.read_parquet(kline_path)
    klines_local_w = klines_local[
        (klines_local["timestamp"] >= start_ms) & (klines_local["timestamp"] < end_ms)
    ].reset_index(drop=True)

    # === 1) 始终先拉 klines.taker_buy_base 算窗口内 delta/cvd ===
    print(f"[klines] 拉取 {symbol} {timeframe} taker_buy: window {window*2+1} 根")
    klines_taker = fetch_klines_with_taker(market, base, quote, timeframe, start_ms, end_ms)
    of = compute_orderflow_from_klines(klines_local_w, klines_taker)

    # === 2) 模式分支 ===
    result = {
        "mode": mode,
        "trade_id": trade_id,
        "window_bars": window * 2 + 1,
        "klines_bars": len(of),
    }

    if mode == "klines":
        of.to_parquet(of_path, index=False)
        print(f"[ok] klines 模式: 落盘 {of_path.name} ({len(of)} 根 K 线，无 aggTrades)")
        result.update({
            "orderflow_path": str(of_path.relative_to(PROJECT_ROOT)),
            "trades_path": None,
            "hotspots_path": None,
            "trades_count": 0,
        })
        return result

    # 合并 features 用于钩子识别
    of_with_features = of.merge(
        klines_local_w.drop(columns=[c for c in ["close", "datetime"] if c in klines_local_w.columns]),
        on=["kline_id", "timestamp"],
        how="left",
    )

    # 锚 K id（默认就是 anchor_cn 对应的 K，约定 review_builder 里编号 A0）
    anchor_bar_id = "A0"

    if mode == "full":
        # 旧行为：整个窗口拉 aggTrades
        if trades_path.exists() and not force_refetch:
            trades = pd.read_parquet(trades_path)
            cached_start = int(trades["timestamp"].min()) if not trades.empty else end_ms
            cached_end = int(trades["timestamp"].max()) if not trades.empty else start_ms
            if cached_start > start_ms or cached_end < end_ms - tf_ms:
                print(f"[info] full 模式缓存窗口不足，重新拉取")
                trades = fetch_trades_window(exchange, market, base, quote, start_ms, end_ms, limit=limit)
                trades.to_parquet(trades_path, index=False)
            else:
                print(f"[cache] full 模式使用缓存: {trades_path.name} ({len(trades)} 笔)")
                trades = trades[(trades["timestamp"] >= start_ms) & (trades["timestamp"] < end_ms)].reset_index(drop=True)
        else:
            print(f"[fetch] full 模式拉取整窗口 aggTrades [{start_ms}, {end_ms})")
            trades = fetch_trades_window(exchange, market, base, quote, start_ms, end_ms, limit=limit)
            trades.to_parquet(trades_path, index=False)
        # full 模式仍以 klines 推算 delta/cvd（一致性）；trades 留作微观分析
        of.to_parquet(of_path, index=False)
        # 一并记录 hotspots（mode=full 时 hotspot 等同全窗口）
        hotspots = {
            "mode": "full",
            "trade_id": trade_id,
            "window_bars": window * 2 + 1,
            "agg_trades_bars": window * 2 + 1,
            "agg_trades_coverage_pct": 100.0,
            "hooks_used": [],
            "buffer_bars": 0,
            "hotspots": [],
            "ranges": [{"start_ts": start_ms, "end_ts": end_ms,
                        "bar_count": window * 2 + 1,
                        "first_bar_id": klines_local_w.iloc[0]["kline_id"],
                        "last_bar_id": klines_local_w.iloc[-1]["kline_id"]}],
        }
        with open(hotspots_path, "w", encoding="utf-8") as f:
            json.dump(hotspots, f, ensure_ascii=False, indent=2, default=str)
        result.update({
            "orderflow_path": str(of_path.relative_to(PROJECT_ROOT)),
            "trades_path": str(trades_path.relative_to(PROJECT_ROOT)),
            "hotspots_path": str(hotspots_path.relative_to(PROJECT_ROOT)),
            "trades_count": len(trades),
        })
        return result

    # === hybrid: 钩子 → 热点 → 拉 aggTrades 段 ===
    hot = detect_hotspots(
        of_with_features,
        hooks=hooks,
        anchor_bar_id=anchor_bar_id,
        extra_bar_ids=extra_bar_ids,
        buffer_bars=buffer_bars,
        thresholds=thresholds,
        tf_ms=tf_ms,
    )
    print(f"[hooks] 启用 {hot.hooks_used}, buffer ±{buffer_bars}")
    print(f"[hotspots] {len(hot.hotspot_bar_ids)} 根命中: {hot.hotspot_bar_ids}")
    print(f"[hotspots] buffer 后 {len(hot.expanded_bar_ids)}/{hot.klines_total} 根 → {len(hot.contiguous_ranges)} 段")

    if hot.contiguous_ranges:
        print(f"[fetch] hybrid 模式拉热点段 aggTrades")
        trades = fetch_trades_for_ranges(exchange, market, base, quote, hot.contiguous_ranges, limit=limit)
    else:
        print("[hybrid] 无热点命中，跳过 aggTrades")
        trades = pd.DataFrame(columns=["timestamp", "price", "amount", "side"])

    if not trades.empty:
        trades.to_parquet(trades_path, index=False)
    elif trades_path.exists():
        # 没命中但有旧缓存，删除避免误用
        trades_path.unlink()

    of.to_parquet(of_path, index=False)

    coverage = (len(hot.expanded_bar_ids) / hot.klines_total * 100) if hot.klines_total else 0.0
    hotspots = {
        "mode": "hybrid",
        "trade_id": trade_id,
        "window_bars": window * 2 + 1,
        "klines_bars_total": hot.klines_total,
        "agg_trades_bars": len(hot.expanded_bar_ids),
        "agg_trades_coverage_pct": round(coverage, 2),
        "hooks_used": hot.hooks_used,
        "thresholds": hot.thresholds,
        "buffer_bars": hot.buffer_bars,
        "hotspots": [
            {
                "bar_id": bid,
                "matched": hot.matches.get(bid, []),
            }
            for bid in hot.hotspot_bar_ids
        ],
        "expanded_bar_ids": hot.expanded_bar_ids,
        "ranges": hot.contiguous_ranges,
    }
    with open(hotspots_path, "w", encoding="utf-8") as f:
        json.dump(hotspots, f, ensure_ascii=False, indent=2, default=str)
    print(f"[ok] hybrid 落盘 {of_path.name} + {trades_path.name if not trades.empty else '(无 trades)'} + {hotspots_path.name}")
    print(f"[ok] aggTrades 覆盖 {coverage:.1f}% bar，节省 {100 - coverage:.1f}% 流量")

    result.update({
        "orderflow_path": str(of_path.relative_to(PROJECT_ROOT)),
        "trades_path": str(trades_path.relative_to(PROJECT_ROOT)) if not trades.empty else None,
        "hotspots_path": str(hotspots_path.relative_to(PROJECT_ROOT)),
        "trades_count": len(trades),
        "hotspot_count": len(hot.hotspot_bar_ids),
        "agg_trades_coverage_pct": round(coverage, 2),
    })
    return result


# ----------------- CLI -----------------

def _main():
    p = argparse.ArgumentParser(description="按需拉取订单流（klines / hybrid / full 三模式）")
    p.add_argument("--date", required=True, help="复盘日期 YYYY-MM-DD")
    p.add_argument("--trade-id", required=True)
    p.add_argument("--window", type=int, default=None, help="锚 K 前后各 N 根（默认读配置）")
    p.add_argument("--mode", default=None, choices=["klines", "hybrid", "full"],
                   help="数据源模式（默认读配置 default_mode=hybrid）")
    p.add_argument("--hooks", default=None,
                   help="逗号分隔的钩子名（默认读配置 default_hooks）")
    p.add_argument("--buffer-bars", type=int, default=None,
                   help="热点 bar 前后各 ±N 根（默认读配置 default_buffer_bars）")
    p.add_argument("--extra-bar-ids", default=None,
                   help="逗号分隔，强制纳入热点的 bar_id（如 A0,A-1）")
    p.add_argument("--force-refetch", action="store_true", help="忽略 trades 缓存，强制重拉")
    args = p.parse_args()

    hooks = [h.strip() for h in args.hooks.split(",") if h.strip()] if args.hooks else None
    extra = [b.strip() for b in args.extra_bar_ids.split(",") if b.strip()] if args.extra_bar_ids else None

    result = build_orderflow(
        review_date=args.date,
        trade_id=args.trade_id,
        window=args.window,
        mode=args.mode,
        hooks=hooks,
        buffer_bars=args.buffer_bars,
        extra_bar_ids=extra,
        force_refetch=args.force_refetch,
    )
    print("[DONE]", result)


if __name__ == "__main__":
    _main()
