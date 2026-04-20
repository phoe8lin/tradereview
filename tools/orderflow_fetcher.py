"""按需拉取锚定 K 附近的 aggTrades，计算订单流指标。

设计要点：
- 默认不在 review_builder 中拉取，仅按需调用本工具。
- 读取已有的 trades/<trade_id>.yaml 拿交易所/标的/周期/锚定时间等元数据。
- 窗口：锚定 K 前后各 N 根（默认 10，共 21 根）。
- 缓存：
  * <trade_id>.trades.parquet  —— 原始 aggTrades（供后续自定义分析）
  * <trade_id>.orderflow.parquet —— K线+订单流指标（日常使用）
  已存在 trades.parquet 则跳过重新拉取，直接重算 orderflow。
- 指标：buy_vol / sell_vol / delta / cvd / buy_ratio（窗口内累积 CVD）。

命令行示例：

/opt/anaconda3/envs/trade/bin/python -m tools.orderflow_fetcher \
    --date 2026-04-20 --trade-id HYPE_5m_001 --window 10
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Optional

import ccxt
import pandas as pd
import yaml

from .config import PROJECT_ROOT, PROXY, load_defaults
from .data_fetcher import _build_exchange, _build_symbol, _TF_MS, TZ_CN


def _load_trade_meta(review_date: str, trade_id: str) -> dict:
    yaml_path = PROJECT_ROOT / "reviews" / review_date / "trades" / f"{trade_id}.yaml"
    if not yaml_path.exists():
        raise FileNotFoundError(f"找不到 trade yaml: {yaml_path}")
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _anchor_ms_from_cn(anchor_cn: str) -> int:
    dt = pd.to_datetime(anchor_cn).tz_localize(TZ_CN)
    return int(dt.tz_convert("UTC").timestamp() * 1000)


def fetch_trades_window(
    exchange_id: str,
    market: str,
    base: str,
    quote: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> pd.DataFrame:
    """拉取 [start_ms, end_ms) 区间内的 aggTrades，ccxt 自动走 binance /fapi/v1/aggTrades。"""
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
            # 限频简单退避
            print(f"[warn] fetch_trades 失败，退避 1s: {e}")
            time.sleep(1.0)
            continue
        if not rows:
            break
        all_rows.extend(rows)
        last_ts = rows[-1]["timestamp"]
        if last_ts <= cursor:  # 防止原地踏步
            break
        cursor = last_ts + 1
        if len(rows) < limit and cursor >= end_ms:
            break

    if not all_rows:
        return pd.DataFrame(columns=["timestamp", "price", "amount", "side"])

    df = pd.DataFrame(
        [
            {
                "timestamp": r["timestamp"],
                "price": float(r["price"]),
                "amount": float(r["amount"]),
                "side": r["side"],  # 'buy' = taker 主动买, 'sell' = taker 主动卖
            }
            for r in all_rows
            if r.get("timestamp") is not None
        ]
    )
    df = df.drop_duplicates(subset=["timestamp", "price", "amount", "side"]).sort_values("timestamp").reset_index(drop=True)
    df = df[(df["timestamp"] >= start_ms) & (df["timestamp"] < end_ms)].reset_index(drop=True)
    return df


def compute_orderflow(klines: pd.DataFrame, trades: pd.DataFrame, tf_ms: int) -> pd.DataFrame:
    """按 K 线时间分桶，计算每根 K 线的订单流指标。"""
    if trades.empty:
        of = klines[["kline_id", "datetime", "timestamp", "close"]].copy()
        for col in ["buy_vol", "sell_vol", "total_trades_vol", "delta", "cvd", "buy_ratio"]:
            of[col] = 0.0
        return of

    # 将每笔 trade 的 timestamp floor 到所属 K 线开始时间
    trades = trades.copy()
    trades["bar_ts"] = (trades["timestamp"] // tf_ms) * tf_ms
    trades["buy_vol"] = trades.apply(
        lambda r: r["amount"] if r["side"] == "buy" else 0.0, axis=1
    )
    trades["sell_vol"] = trades.apply(
        lambda r: r["amount"] if r["side"] == "sell" else 0.0, axis=1
    )

    grp = trades.groupby("bar_ts").agg(
        buy_vol=("buy_vol", "sum"),
        sell_vol=("sell_vol", "sum"),
    ).reset_index()
    grp["total_trades_vol"] = grp["buy_vol"] + grp["sell_vol"]
    grp["delta"] = grp["buy_vol"] - grp["sell_vol"]
    grp = grp.rename(columns={"bar_ts": "timestamp"})

    # 合并到 K 线
    of = klines[["kline_id", "datetime", "timestamp", "close"]].merge(
        grp, on="timestamp", how="left"
    )
    of[["buy_vol", "sell_vol", "total_trades_vol", "delta"]] = of[
        ["buy_vol", "sell_vol", "total_trades_vol", "delta"]
    ].fillna(0.0)
    # 窗口内累积 CVD
    of["cvd"] = of["delta"].cumsum()
    # buy_ratio 0~1（总量为 0 时填 NaN）
    of["buy_ratio"] = of.apply(
        lambda r: (r["buy_vol"] / r["total_trades_vol"]) if r["total_trades_vol"] > 0 else float("nan"),
        axis=1,
    )
    return of


def build_orderflow(
    review_date: str,
    trade_id: str,
    window: Optional[int] = None,
    force_refetch: bool = False,
) -> dict:
    """主入口：读 yaml -> 拉/读 trades -> 计算 orderflow -> 落盘。"""
    cfg = load_defaults()
    if window is None:
        window = cfg["orderflow"]["default_window"]
    limit = cfg["orderflow"]["trades_limit"]

    meta = _load_trade_meta(review_date, trade_id)
    exchange = meta["exchange"]
    market = meta["market"]
    symbol = meta["symbol"]                  # e.g. HYPE/USDT
    timeframe = meta["timeframe"]
    anchor_cn = meta["anchor_cn"]
    base, quote = symbol.split(":")[0].split("/")  # 兼容 okx 的 BTC/USDT:USDT

    if timeframe not in _TF_MS:
        raise ValueError(f"不支持的周期: {timeframe}")
    tf_ms = _TF_MS[timeframe]

    anchor_ms = _anchor_ms_from_cn(anchor_cn)
    start_ms = anchor_ms - window * tf_ms
    end_ms = anchor_ms + (window + 1) * tf_ms   # +1 覆盖锚 K 自身的区间

    data_dir = PROJECT_ROOT / "reviews" / review_date / "data"
    kline_path = data_dir / f"{trade_id}.parquet"
    trades_path = data_dir / f"{trade_id}.trades.parquet"
    of_path = data_dir / f"{trade_id}.orderflow.parquet"

    # 1) 拿 trades
    if trades_path.exists() and not force_refetch:
        trades = pd.read_parquet(trades_path)
        # 缓存文件的窗口可能比当前请求更小，需检查覆盖范围
        cached_start = int(trades["timestamp"].min()) if not trades.empty else end_ms
        cached_end = int(trades["timestamp"].max()) if not trades.empty else start_ms
        if cached_start > start_ms or cached_end < end_ms - tf_ms:
            print(f"[info] 缓存 trades 窗口不足，重新拉取 (cache {cached_start}~{cached_end} vs need {start_ms}~{end_ms})")
            trades = fetch_trades_window(exchange, market, base, quote, start_ms, end_ms, limit=limit)
            trades.to_parquet(trades_path, index=False)
        else:
            print(f"[cache] 使用已有 trades 缓存: {trades_path.name} ({len(trades)} 笔)")
            # 截窗到当前需要的区间
            trades = trades[(trades["timestamp"] >= start_ms) & (trades["timestamp"] < end_ms)].reset_index(drop=True)
    else:
        print(f"[fetch] 拉取 {symbol} {timeframe} trades: window [{start_ms}, {end_ms}) ≈ {window*2+1} 根 K 线")
        trades = fetch_trades_window(exchange, market, base, quote, start_ms, end_ms, limit=limit)
        trades.to_parquet(trades_path, index=False)
        print(f"[ok] 落盘 {trades_path.name} ({len(trades)} 笔 aggTrades)")

    # 2) 读 K 线（parquet 里 datetime 是 naive，原先是 UTC+8）
    klines = pd.read_parquet(kline_path)
    # 只保留窗口内 K 线
    klines_w = klines[
        (klines["timestamp"] >= start_ms) & (klines["timestamp"] < end_ms)
    ].reset_index(drop=True)

    # 3) 计算订单流
    of = compute_orderflow(klines_w, trades, tf_ms)
    of.to_parquet(of_path, index=False)
    print(f"[ok] 落盘 {of_path.name} ({len(of)} 根 K 线)")

    return {
        "trades_path": str(trades_path.relative_to(PROJECT_ROOT)),
        "orderflow_path": str(of_path.relative_to(PROJECT_ROOT)),
        "trades_count": len(trades),
        "bars_count": len(of),
        "window": window,
    }


def _main():
    p = argparse.ArgumentParser(description="按需拉取订单流数据并计算 Delta/CVD")
    p.add_argument("--date", required=True, help="复盘日期 YYYY-MM-DD")
    p.add_argument("--trade-id", required=True)
    p.add_argument("--window", type=int, default=None, help="锚 K 前后各 N 根（默认读配置）")
    p.add_argument("--force-refetch", action="store_true", help="忽略 trades 缓存，强制重拉")
    args = p.parse_args()
    result = build_orderflow(
        review_date=args.date,
        trade_id=args.trade_id,
        window=args.window,
        force_refetch=args.force_refetch,
    )
    print("[DONE]", result)


if __name__ == "__main__":
    _main()
