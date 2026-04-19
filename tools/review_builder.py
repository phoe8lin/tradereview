"""一键生成复盘包：拉数据 → 算指标/特征 → 编号 → 绘图 → 落 trade.yaml。

命令行示例：

/opt/anaconda3/envs/trade/bin/python -m tools.review_builder \
    --date 2026-04-19 --trade-id BTC_1h_001 \
    --base BTC --quote USDT --timeframe 1h \
    --anchor "2026-04-18 14:00" \
    --entry 62340 --stop 61800 --take 64000 --direction long \
    --notes "均线多头回踩EMA55，Wave 超卖上穿"
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

from .config import PROJECT_ROOT, ensure_dirs, load_defaults
from .chart_replicator import build_chart
from .data_fetcher import FetchSpec, fetch_around_anchor
from .indicators import (
    add_ema,
    add_wave_filter,
    classify_ema_stack,
    classify_wave_state,
)
from .kline_features import add_kline_features
from .kline_indexer import assign_kline_index


@dataclass
class TradeMeta:
    date: str
    trade_id: str
    exchange: str
    market: str
    symbol: str
    timeframe: str
    anchor_cn: str
    direction: str
    entry: Optional[float]
    stop: Optional[float]
    take: Optional[float]
    rr: Optional[float]
    anchor_kline_id: str
    ema_stack_at_anchor: str
    wave_state_at_anchor: str
    wave_value_at_anchor: Optional[float]
    notes: str
    raw_screenshot: Optional[str]
    data_path: str
    chart_path: str


def _compute_rr(direction: str, entry: Optional[float], stop: Optional[float], take: Optional[float]) -> Optional[float]:
    if entry is None or stop is None or take is None:
        return None
    risk = abs(entry - stop)
    reward = abs(take - entry)
    if risk == 0:
        return None
    sign = 1 if direction.lower() == "long" else -1
    # 方向校验（软性）：多单止损应低于入场，止盈应高于入场
    return round(reward / risk, 3)


def build_review(
    review_date: str,
    trade_id: str,
    symbol_base: str,
    symbol_quote: str,
    timeframe: str,
    anchor_time: str,
    entry: Optional[float] = None,
    stop: Optional[float] = None,
    take: Optional[float] = None,
    direction: str = "long",
    exchange: str = "binance",
    market: str = "futures",
    notes: str = "",
    raw_screenshot: Optional[str] = None,
) -> TradeMeta:
    ensure_dirs()
    cfg = load_defaults()

    # 目录
    review_root = PROJECT_ROOT / "reviews" / review_date
    (review_root / "raw").mkdir(parents=True, exist_ok=True)
    (review_root / "data").mkdir(parents=True, exist_ok=True)
    (review_root / "replicated").mkdir(parents=True, exist_ok=True)
    (review_root / "trades").mkdir(parents=True, exist_ok=True)
    review_md = review_root / "review.md"
    if not review_md.exists():
        review_md.write_text(f"# {review_date} 复盘\n\n", encoding="utf-8")

    # 1) 数据
    bars_before = cfg["data"]["warmup_bars"] + cfg["data"]["display_bars_before"]
    bars_after = cfg["data"]["display_bars_after"]
    spec = FetchSpec(
        exchange=exchange,
        market=market,
        base=symbol_base,
        quote=symbol_quote,
        timeframe=timeframe,
        anchor_cn=anchor_time,
        bars_before=bars_before,
        bars_after=bars_after,
    )
    df = fetch_around_anchor(spec)

    # 2) 指标
    add_ema(df, cfg["ema"]["periods"])
    wf = cfg["wave_filter"]
    add_wave_filter(
        df,
        rsi_period=wf["rsi_period"],
        stoch_period=wf["stoch_period"],
        smooth_k=wf["smooth_k"],
        smooth_d=wf["smooth_d"],
        ma_type=wf["ma_type"],
        source=wf["source"],
    )

    # 3) 形态特征
    add_kline_features(df)

    # 4) 编号
    assign_kline_index(df, anchor_ms=df.attrs["anchor_ms"])

    # 5) 落 parquet
    data_path = review_root / "data" / f"{trade_id}.parquet"
    df_to_save = df.copy()
    df_to_save["datetime"] = df_to_save["datetime"].dt.tz_localize(None)  # parquet 对 tz 不友好
    df_to_save.to_parquet(data_path, index=False)

    # 6) 绘图
    chart_path = review_root / "replicated" / f"{trade_id}.html"
    title = f"{exchange.upper()} {spec.base}/{spec.quote} {'PERP' if market == 'futures' else 'SPOT'} · {timeframe} · anchor {anchor_time}"
    build_chart(
        df,
        title=title,
        entry=entry,
        stop=stop,
        take=take,
        direction=direction,
        output_html=str(chart_path),
    )

    # 7) 截图自动拾取：若未显式指定 raw_screenshot，则按约定在 raw/ 目录下查找
    #    约定文件名：<trade_id>.{png,jpg,jpeg,webp}
    if not raw_screenshot:
        for ext in ("png", "jpg", "jpeg", "webp"):
            candidate = review_root / "raw" / f"{trade_id}.{ext}"
            if candidate.exists():
                raw_screenshot = str(candidate.relative_to(PROJECT_ROOT))
                break

    # 8) 元数据
    anchor_row = df[df["is_anchor"]].iloc[0]
    meta = TradeMeta(
        date=review_date,
        trade_id=trade_id,
        exchange=exchange,
        market=market,
        symbol=df.attrs["symbol"],
        timeframe=timeframe,
        anchor_cn=anchor_time,
        direction=direction,
        entry=entry,
        stop=stop,
        take=take,
        rr=_compute_rr(direction, entry, stop, take),
        anchor_kline_id=str(anchor_row["kline_id"]),
        ema_stack_at_anchor=classify_ema_stack(anchor_row, tuple(cfg["ema"]["periods"])),
        wave_state_at_anchor=classify_wave_state(
            anchor_row["wave"], wf["overbought"], wf["oversold"]
        ),
        wave_value_at_anchor=None if pd.isna(anchor_row["wave"]) else round(float(anchor_row["wave"]), 2),
        notes=notes,
        raw_screenshot=raw_screenshot,
        data_path=str(data_path.relative_to(PROJECT_ROOT)),
        chart_path=str(chart_path.relative_to(PROJECT_ROOT)),
    )

    trade_yaml = review_root / "trades" / f"{trade_id}.yaml"
    with open(trade_yaml, "w", encoding="utf-8") as f:
        yaml.safe_dump(asdict(meta), f, allow_unicode=True, sort_keys=False)

    # 追加 review.md 快照
    with open(review_md, "a", encoding="utf-8") as f:
        f.write(
            f"\n## [{trade_id}] {meta.symbol} {timeframe}  anchor={anchor_time}\n"
            f"- 方向 {direction} · 入场 {entry} · 止损 {stop} · 止盈 {take} · RR {meta.rr}\n"
            f"- 锚 K: {meta.anchor_kline_id} · EMA 结构: {meta.ema_stack_at_anchor} · Wave: {meta.wave_state_at_anchor} ({meta.wave_value_at_anchor})\n"
            f"- 备注: {notes}\n"
            f"- 图表: {meta.chart_path}\n"
        )
        # 若存在原始截图，以 Markdown 图片嵌入（review.md 相对于 reviews/<date>/ 引用 raw/xxx）
        if meta.raw_screenshot:
            # meta.raw_screenshot 是相对 PROJECT_ROOT 的路径，
            # review.md 位于 reviews/<date>/，需要把前缀 reviews/<date>/ 去掉
            rel = meta.raw_screenshot
            prefix = f"reviews/{review_date}/"
            if rel.startswith(prefix):
                rel = rel[len(prefix):]
            f.write(f"\n![{trade_id} 原始截图]({rel})\n")

    return meta


def _main():
    p = argparse.ArgumentParser(description="生成单次复盘包")
    p.add_argument("--date", required=True, help="复盘日期 YYYY-MM-DD")
    p.add_argument("--trade-id", required=True)
    p.add_argument("--base", required=True)
    p.add_argument("--quote", default="USDT")
    p.add_argument("--timeframe", default="1h")
    p.add_argument("--anchor", required=True, help="锚定 K 线时间 UTC+8, 如 2026-04-18 14:00")
    p.add_argument("--entry", type=float, default=None)
    p.add_argument("--stop", type=float, default=None)
    p.add_argument("--take", type=float, default=None)
    p.add_argument("--direction", default="long", choices=["long", "short"])
    p.add_argument("--exchange", default="binance")
    p.add_argument("--market", default="futures", choices=["spot", "futures"])
    p.add_argument("--notes", default="")
    p.add_argument("--screenshot", default=None)
    args = p.parse_args()

    meta = build_review(
        review_date=args.date,
        trade_id=args.trade_id,
        symbol_base=args.base,
        symbol_quote=args.quote,
        timeframe=args.timeframe,
        anchor_time=args.anchor,
        entry=args.entry,
        stop=args.stop,
        take=args.take,
        direction=args.direction,
        exchange=args.exchange,
        market=args.market,
        notes=args.notes,
        raw_screenshot=args.screenshot,
    )
    print("[OK]", meta.trade_id, "->", meta.chart_path)


if __name__ == "__main__":
    _main()
