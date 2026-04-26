"""多笔相邻交易合并绘图：把同标的/同周期/时间相近的若干笔 trade 画到一张 HTML。

典型场景：上午 14:00/15:50/20:10 三笔 HYPE 5m 多单 → 一张图上同时标注入场/止损/止盈，
便于对比首仓 vs 加仓 vs 回踩。

按需抓取 CVD（--with-cvd），不在分析范围内的订单流数据不会浪费网络。

命令行示例：

    /opt/anaconda3/envs/trade/bin/python -m tools.multi_trade_chart \
        --date 2026-04-22 \
        --trade-ids HYPE_5m_002,HYPE_5m_003,HYPE_5m_004 \
        --output combined_afternoon_longs \
        --with-cvd

产物：
    reviews/<date>/replicated/<output>.html
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import yaml
from plotly.subplots import make_subplots

from .config import PROJECT_ROOT, load_defaults
from .data_fetcher import FetchSpec, fetch_around_anchor, TZ_CN, _TF_MS
from .indicators import add_ema, add_wave_filter
from .kline_features import add_kline_features
from .orderflow_fetcher import build_orderflow


TRADE_COLORS = [
    "#1e88e5",  # blue
    "#8e24aa",  # purple
    "#e53935",  # red
    "#43a047",  # green
    "#fb8c00",  # orange
]


def _load_yaml(review_date: str, trade_id: str) -> dict:
    path = PROJECT_ROOT / "reviews" / review_date / "trades" / f"{trade_id}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"未找到 trade yaml: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _anchor_ts(anchor_cn: str) -> pd.Timestamp:
    return pd.Timestamp(anchor_cn).tz_localize(TZ_CN)


def _load_or_fetch_klines(
    review_date: str,
    base: str,
    quote: str,
    timeframe: str,
    exchange: str,
    market: str,
    start_cn: pd.Timestamp,
    end_cn: pd.Timestamp,
    cfg: dict,
) -> pd.DataFrame:
    """优先复用 day_<BASE>_<TF>.parquet（含 CVD），否则按合集窗口重新拉取。"""
    day_path = (
        PROJECT_ROOT / "reviews" / review_date / "data" / f"day_{base.upper()}_{timeframe}.parquet"
    )
    if day_path.exists():
        df = pd.read_parquet(day_path)
        df["datetime"] = pd.to_datetime(df["datetime"])
        # day parquet 内 datetime 已是 naive，当作 UTC+8 处理
        if df["datetime"].dt.tz is None:
            df["datetime"] = df["datetime"].dt.tz_localize(TZ_CN)
        # kline_id 列名在 day parquet 中叫 "id"
        if "id" in df.columns and "kline_id" not in df.columns:
            df = df.rename(columns={"id": "kline_id"})
        print(f"[reuse] {day_path.relative_to(PROJECT_ROOT)} ({len(df)} 根)")
        return df

    # 无 day parquet 则合集拉取
    mid = start_cn + (end_cn - start_cn) / 2
    anchor_cn = mid.strftime("%Y-%m-%d %H:%M")
    tf_ms = _TF_MS[timeframe]
    bars_before = cfg["data"]["warmup_bars"] + max(
        int((mid - start_cn) / pd.Timedelta(tf_ms, unit="ms")) + 20, 20
    )
    bars_after = int((end_cn - mid) / pd.Timedelta(tf_ms, unit="ms")) + 20
    spec = FetchSpec(
        exchange=exchange, market=market, base=base, quote=quote,
        timeframe=timeframe, anchor_cn=anchor_cn,
        bars_before=bars_before, bars_after=bars_after,
    )
    df = fetch_around_anchor(spec)
    add_ema(df, cfg["ema"]["periods"])
    wf = cfg["wave_filter"]
    add_wave_filter(
        df,
        rsi_period=wf["rsi_period"], stoch_period=wf["stoch_period"],
        smooth_k=wf["smooth_k"], smooth_d=wf["smooth_d"],
        ma_type=wf["ma_type"], source=wf["source"],
    )
    add_kline_features(df)
    return df


def _ensure_cvd(df: pd.DataFrame, review_date: str, trade_ids: list[str]) -> pd.DataFrame:
    """确保 df 中含 cvd 列。day parquet 已含；单笔模式则按需为每个 trade 调 orderflow_fetcher 合并。"""
    if "cvd" in df.columns:
        return df
    # 没有 CVD 列 → 每个 trade 各拉一次订单流，按时间戳合并
    all_of = []
    for tid in trade_ids:
        info = build_orderflow(review_date=review_date, trade_id=tid)
        of = pd.read_parquet(PROJECT_ROOT / info["orderflow_path"])
        all_of.append(of[["timestamp", "delta"]])
    merged = pd.concat(all_of).drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
    df = df.merge(merged, on="timestamp", how="left")
    df["delta"] = df["delta"].fillna(0.0)
    df["cvd"] = df["delta"].cumsum()
    return df


def build_multi_chart(
    review_date: str,
    trade_ids: list[str],
    output_name: str,
    with_cvd: bool = False,
    padding_bars: int = 10,
) -> str:
    cfg = load_defaults()
    if not trade_ids:
        raise ValueError("trade_ids 不能为空")

    metas = [_load_yaml(review_date, tid) for tid in trade_ids]

    # 一致性校验
    ref = metas[0]
    for m in metas[1:]:
        for k in ("exchange", "market", "symbol", "timeframe"):
            if m[k] != ref[k]:
                raise ValueError(f"trade {m['trade_id']} 的 {k}={m[k]} 与首个 {ref[k]} 不一致，不能合并绘图")

    timeframe = ref["timeframe"]
    base, quote_sym = ref["symbol"].split(":")[0].split("/")
    tf_ms = _TF_MS[timeframe]

    # 合集窗口
    anchors = sorted([_anchor_ts(m["anchor_cn"]) for m in metas])
    start_cn = anchors[0] - padding_bars * pd.Timedelta(tf_ms, unit="ms")
    end_cn = anchors[-1] + padding_bars * pd.Timedelta(tf_ms, unit="ms")

    # 加载数据
    df = _load_or_fetch_klines(
        review_date, base, quote_sym, timeframe,
        ref["exchange"], ref["market"], start_cn, end_cn, cfg,
    )

    # 可选 CVD
    if with_cvd:
        df = _ensure_cvd(df, review_date, trade_ids)

    # 截到合集显示窗口
    show = df[(df["datetime"] >= start_cn) & (df["datetime"] <= end_cn)].copy()
    if show.empty:
        raise RuntimeError("合集窗口内无数据，检查日期/标的/周期")

    # --- 绘图 ---
    rows = 3 if with_cvd else 2
    row_heights = [0.60, 0.22, 0.18] if with_cvd else [0.75, 0.25]
    subtitle = "Wave Filter"
    titles = [
        f"{ref['exchange'].upper()} {ref['symbol']} {timeframe} · 多笔合并 ({len(metas)})",
        subtitle,
    ]
    if with_cvd:
        titles.append("CVD (累计 Delta)")

    fig = make_subplots(
        rows=rows, cols=1, shared_xaxes=True,
        row_heights=row_heights, vertical_spacing=0.03,
        subplot_titles=titles,
    )

    chart_cfg = cfg["chart"]
    fig.add_trace(
        go.Candlestick(
            x=show["datetime"], open=show["open"], high=show["high"],
            low=show["low"], close=show["close"],
            increasing_line_color=chart_cfg["candle_up_color"],
            decreasing_line_color=chart_cfg["candle_down_color"],
            increasing_fillcolor=chart_cfg["candle_up_color"],
            decreasing_fillcolor=chart_cfg["candle_down_color"],
            name="K",
        ),
        row=1, col=1,
    )

    # EMA
    for p in cfg["ema"]["periods"]:
        col = f"ema_{p}"
        if col in show.columns:
            fig.add_trace(
                go.Scatter(
                    x=show["datetime"], y=show[col], mode="lines",
                    line=dict(width=1.1, color=cfg["ema"]["colors"].get(p, "#999")),
                    name=f"EMA{p}",
                ),
                row=1, col=1,
            )

    # 编号标注（每 step 根一个）
    step = max(1, len(show) // 40)
    ann = show.iloc[::step]
    if "kline_id" in show.columns:
        fig.add_trace(
            go.Scatter(
                x=ann["datetime"], y=ann["high"] * 1.001, mode="text",
                text=ann["kline_id"], textfont=dict(size=9, color="#888"),
                showlegend=False, hoverinfo="skip",
            ),
            row=1, col=1,
        )

    # --- 每笔 trade 的 entry/stop/take + 锚 K 竖线 ---
    x_right = show["datetime"].iloc[-1]
    for idx, m in enumerate(metas):
        color = TRADE_COLORS[idx % len(TRADE_COLORS)]
        label_num = f"①②③④⑤"[idx] if idx < 5 else f"#{idx+1}"
        anchor_dt = _anchor_ts(m["anchor_cn"])
        # 竖线 + 顶部标签
        fig.add_vline(
            x=anchor_dt,
            line=dict(color=color, width=2, dash="solid"),
            opacity=0.55, row=1, col=1,
        )
        fig.add_annotation(
            x=anchor_dt, y=show["high"].max(),
            text=f"{label_num} {m['trade_id']} ({m['direction'].upper()})",
            showarrow=False, yshift=22,
            font=dict(color=color, size=10),
            row=1, col=1,
        )
        # entry/stop/take 只从 anchor 开始画一段（而不是贯穿全图）
        for kind, y, dash in [
            ("Entry", m.get("entry"), "solid"),
            ("Stop", m.get("stop"), "dash"),
            ("Take", m.get("take"), "dash"),
        ]:
            if y is None:
                continue
            fig.add_shape(
                type="line", xref="x", yref="y",
                x0=anchor_dt, x1=x_right, y0=y, y1=y,
                line=dict(color=color, width=1.4, dash=dash),
                row=1, col=1,
            )
            fig.add_annotation(
                x=x_right, y=y, xref="x", yref="y",
                text=f"{label_num}{kind} {y}",
                showarrow=False, xanchor="left",
                font=dict(color=color, size=9),
                row=1, col=1,
            )

    # --- Wave 子图 ---
    wf = cfg["wave_filter"]
    ob, os_ = wf["overbought"], wf["oversold"]
    n = len(show)
    fig.add_trace(go.Scatter(x=show["datetime"], y=[ob]*n, mode="lines",
                             line=dict(width=0), showlegend=False, hoverinfo="skip"), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=show["datetime"], y=show["wave"].clip(lower=ob), mode="lines",
        line=dict(width=0), fill="tonexty", fillcolor="rgba(211,47,47,0.35)",
        showlegend=False, hoverinfo="skip",
    ), row=2, col=1)
    fig.add_trace(go.Scatter(x=show["datetime"], y=[os_]*n, mode="lines",
                             line=dict(width=0), showlegend=False, hoverinfo="skip"), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=show["datetime"], y=show["wave"].clip(upper=os_), mode="lines",
        line=dict(width=0), fill="tonexty", fillcolor="rgba(46,125,50,0.35)",
        showlegend=False, hoverinfo="skip",
    ), row=2, col=1)
    fig.add_trace(go.Scatter(x=show["datetime"], y=show["wave"], mode="lines",
                             line=dict(color="#4b4e50", width=1.4, shape="hv"), name="Wave"),
                  row=2, col=1)
    fig.add_hline(y=0, line=dict(color="#888", dash="dot", width=1), row=2, col=1)
    fig.add_hline(y=ob, line=dict(color="#d32f2f", width=1), row=2, col=1)
    fig.add_hline(y=os_, line=dict(color="#2e7d32", width=1), row=2, col=1)
    fig.update_yaxes(range=[-65, 65], row=2, col=1)

    # --- CVD 子图（可选）---
    if with_cvd and "cvd" in show.columns:
        fig.add_trace(
            go.Scatter(
                x=show["datetime"], y=show["cvd"], mode="lines",
                line=dict(color="#5e35b1", width=1.4),
                name="CVD",
                fill="tozeroy", fillcolor="rgba(94,53,177,0.12)",
            ),
            row=3, col=1,
        )
        fig.add_hline(y=0, line=dict(color="#888", dash="dot", width=1), row=3, col=1)
        # 在每个 anchor 处画竖线
        for idx, m in enumerate(metas):
            color = TRADE_COLORS[idx % len(TRADE_COLORS)]
            fig.add_vline(
                x=_anchor_ts(m["anchor_cn"]),
                line=dict(color=color, width=1.2, dash="dot"),
                opacity=0.5, row=3, col=1,
            )

    fig.update_layout(
        height=900 if with_cvd else 780,
        template="plotly_white",
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        legend=dict(orientation="h", y=1.02, x=0),
        margin=dict(l=40, r=90, t=60, b=30),
    )
    fig.update_xaxes(showspikes=True, spikethickness=1, spikedash="dot")
    fig.update_yaxes(showspikes=True, spikethickness=1, spikedash="dot")

    out_dir = PROJECT_ROOT / "reviews" / review_date / "replicated"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{output_name}.html"
    fig.write_html(out_path, include_plotlyjs="cdn")
    print(f"[OK] 合并图 -> {out_path.relative_to(PROJECT_ROOT)}")
    return str(out_path)


def _main():
    p = argparse.ArgumentParser(description="多笔相邻交易合并绘图")
    p.add_argument("--date", required=True, help="复盘日期 YYYY-MM-DD")
    p.add_argument("--trade-ids", required=True, help="逗号分隔的 trade_id 列表")
    p.add_argument("--output", required=True, help="输出 HTML 文件名（不含 .html 后缀）")
    p.add_argument("--with-cvd", action="store_true", help="附加 CVD 子图（复用 day parquet 或按需拉订单流）")
    p.add_argument("--padding-bars", type=int, default=10, help="首/末 anchor 外延 K 根数（默认 10）")
    args = p.parse_args()
    trade_ids = [x.strip() for x in args.trade_ids.split(",") if x.strip()]
    build_multi_chart(
        review_date=args.date,
        trade_ids=trade_ids,
        output_name=args.output,
        with_cvd=args.with_cvd,
        padding_bars=args.padding_bars,
    )


if __name__ == "__main__":
    _main()
