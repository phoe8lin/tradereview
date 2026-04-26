"""Plotly 交互式图表：复刻 TV 风格，包含编号 K 线 + EMA 组 + Wave Filter + RR 标注。

输出单文件 HTML，无需服务器，离线可看。
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .config import load_defaults


def _hover_text(row: pd.Series) -> str:
    dt = row["datetime"].strftime("%Y-%m-%d %H:%M")
    lines = [
        f"<b>{row['kline_id'] or '-'}</b>  {dt}",
        f"O {row['open']:.4f}   H {row['high']:.4f}",
        f"L {row['low']:.4f}   C {row['close']:.4f}",
        f"Vol {row['volume']:.2f}",
    ]
    if pd.notna(row.get("wave")):
        lines.append(f"Wave {row['wave']:.2f}")
    if row.get("upper_wick_ratio") is not None and pd.notna(row["upper_wick_ratio"]):
        lines.append(
            f"wick U/L {row['upper_wick_ratio']:.2f}/{row['lower_wick_ratio']:.2f}  body {row['body_ratio']:.2f}"
        )
    if row.get("vol_vs_ma") is not None and pd.notna(row["vol_vs_ma"]):
        lines.append(f"vol/ma {row['vol_vs_ma']:.2f}")
    return "<br>".join(lines)


def build_chart(
    df: pd.DataFrame,
    title: str,
    entry: Optional[float] = None,
    stop: Optional[float] = None,
    take: Optional[float] = None,
    direction: str = "long",
    output_html: str = "chart.html",
    label_every: int = 5,
) -> str:
    """绘制并保存 HTML，返回文件路径。"""
    cfg = load_defaults()
    ema_cfg = cfg["ema"]
    chart_cfg = cfg["chart"]
    wf = cfg["wave_filter"]

    # 仅展示 in_display 窗口
    show = df[df["in_display"]].copy()
    show["hover"] = show.apply(_hover_text, axis=1)

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.72, 0.28],
        vertical_spacing=0.03,
        subplot_titles=(title, "Wave Filter (StochRSI 变种)"),
    )

    # --- K 线 ---
    fig.add_trace(
        go.Candlestick(
            x=show["datetime"],
            open=show["open"],
            high=show["high"],
            low=show["low"],
            close=show["close"],
            increasing_line_color=chart_cfg["candle_up_color"],
            decreasing_line_color=chart_cfg["candle_down_color"],
            increasing_fillcolor=chart_cfg["candle_up_color"],
            decreasing_fillcolor=chart_cfg["candle_down_color"],
            text=show["hover"],
            hoverinfo="text",
            name="K",
        ),
        row=1,
        col=1,
    )

    # --- K 线编号：每 label_every 根标一次 + anchor K 强制显示） ---
    step = max(1, label_every)
    mask_periodic = (np.arange(len(show)) % step) == 0
    is_anchor = show["is_anchor"].to_numpy() if "is_anchor" in show.columns else np.zeros(len(show), dtype=bool)
    annotate = show[mask_periodic | is_anchor]
    fig.add_trace(
        go.Scatter(
            x=annotate["datetime"],
            y=annotate["high"] * 1.002,
            mode="text",
            text=annotate["kline_id"],
            textfont=dict(size=9, color="#888"),
            showlegend=False,
            hoverinfo="skip",
        ),
        row=1,
        col=1,
    )

    # --- EMA 组 ---
    for p in ema_cfg["periods"]:
        col = f"ema_{p}"
        if col not in show.columns:
            continue
        fig.add_trace(
            go.Scatter(
                x=show["datetime"],
                y=show[col],
                mode="lines",
                line=dict(width=1.2, color=ema_cfg["colors"].get(p, "#999")),
                name=f"EMA{p}",
            ),
            row=1,
            col=1,
        )

    # --- 锚定 K 线高亮 ---
    anchor_row = show[show["is_anchor"]]
    if not anchor_row.empty:
        a = anchor_row.iloc[0]
        fig.add_vline(
            x=a["datetime"],
            line=dict(color=chart_cfg["highlight_color"], width=12),
            opacity=0.35,
            row=1,
            col=1,
        )
        fig.add_annotation(
            x=a["datetime"],
            y=a["high"],
            text=f"★ {a['kline_id']}",
            showarrow=False,
            yshift=18,
            font=dict(color="#d4a017", size=11),
            row=1,
            col=1,
        )

    # --- 入场 / 止损 / 止盈 + 盈亏区（标签标出 R / RR） ---
    risk = abs(entry - stop) if (entry is not None and stop is not None) else None
    rr = round(abs(take - entry) / risk, 3) if (take is not None and risk) else None
    if entry is not None:
        fig.add_hline(
            y=entry,
            line=dict(color=chart_cfg["entry_color"], width=1.5, dash="solid"),
            annotation_text=f"Entry {entry}",
            annotation_position="right",
            row=1,
            col=1,
        )
    if stop is not None:
        stop_lbl = f"Stop {stop}" + (f"  R {risk:.3f}" if risk else "")
        fig.add_hline(
            y=stop,
            line=dict(color=chart_cfg["stop_color"], width=1.5, dash="dash"),
            annotation_text=stop_lbl,
            annotation_position="right",
            row=1,
            col=1,
        )
    if take is not None:
        take_lbl = f"Take {take}" + (f"  RR {rr}" if rr is not None else "")
        fig.add_hline(
            y=take,
            line=dict(color=chart_cfg["take_color"], width=1.5, dash="dash"),
            annotation_text=take_lbl,
            annotation_position="right",
            row=1,
            col=1,
        )

    # 盈亏矩形（从入场 K 线开始往右延伸至展示末端）
    if entry is not None and (stop is not None or take is not None) and not anchor_row.empty:
        x0 = anchor_row.iloc[0]["datetime"]
        x1 = show["datetime"].iloc[-1]
        if stop is not None:
            y_loss_top = max(entry, stop)
            y_loss_bot = min(entry, stop)
            fig.add_shape(
                type="rect",
                xref="x", yref="y",
                x0=x0, x1=x1, y0=y_loss_bot, y1=y_loss_top,
                fillcolor=chart_cfg["loss_zone_color"],
                line=dict(width=0),
                layer="below",
                row=1, col=1,
            )
        if take is not None:
            y_profit_top = max(entry, take)
            y_profit_bot = min(entry, take)
            fig.add_shape(
                type="rect",
                xref="x", yref="y",
                x0=x0, x1=x1, y0=y_profit_bot, y1=y_profit_top,
                fillcolor=chart_cfg["profit_zone_color"],
                line=dict(width=0),
                layer="below",
                row=1, col=1,
            )

    # --- Wave Filter 子图 ---
    # 填充必须先画（基线在前，带 fill 的 trace 在后并 fill=tonexty 到上一条）
    ob = wf["overbought"]
    os_ = wf["oversold"]
    n = len(show)
    # 超买填充：基线 y=ob，顶线 = wave 在 >=ob 处保留，否则 clamp 回 ob（不露出填充）
    fig.add_trace(
        go.Scatter(
            x=show["datetime"], y=[ob] * n, mode="lines",
            line=dict(width=0), showlegend=False, hoverinfo="skip",
        ),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=show["datetime"],
            y=show["wave"].clip(lower=ob),
            mode="lines",
            line=dict(width=0),
            fill="tonexty",
            fillcolor="rgba(211,47,47,0.35)",
            showlegend=False, hoverinfo="skip",
            name="OB fill",
        ),
        row=2, col=1,
    )
    # 超卖填充：基线 y=os，底线 = wave 在 <=os 处保留，否则 clamp 回 os
    fig.add_trace(
        go.Scatter(
            x=show["datetime"], y=[os_] * n, mode="lines",
            line=dict(width=0), showlegend=False, hoverinfo="skip",
        ),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=show["datetime"],
            y=show["wave"].clip(upper=os_),
            mode="lines",
            line=dict(width=0),
            fill="tonexty",
            fillcolor="rgba(46,125,50,0.35)",
            showlegend=False, hoverinfo="skip",
            name="OS fill",
        ),
        row=2, col=1,
    )
    # 主线最后画，压在填充层之上
    fig.add_trace(
        go.Scatter(
            x=show["datetime"],
            y=show["wave"],
            mode="lines",
            line=dict(color="#4b4e50", width=1.5, shape="hv"),
            name="Wave",
        ),
        row=2, col=1,
    )
    fig.add_hline(y=0, line=dict(color="#888", dash="dot", width=1), row=2, col=1)
    fig.add_hline(y=ob, line=dict(color="#d32f2f", width=1), row=2, col=1)
    fig.add_hline(y=os_, line=dict(color="#2e7d32", width=1), row=2, col=1)
    fig.update_yaxes(range=[-65, 65], row=2, col=1)

    # --- Layout ---
    fig.update_layout(
        height=820,
        template="plotly_white",
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        legend=dict(orientation="h", y=1.02, x=0),
        margin=dict(l=40, r=60, t=50, b=30),
    )
    fig.update_xaxes(showspikes=True, spikethickness=1, spikedash="dot")
    fig.update_yaxes(showspikes=True, spikethickness=1, spikedash="dot")

    fig.write_html(output_html, include_plotlyjs="cdn")
    return output_html
