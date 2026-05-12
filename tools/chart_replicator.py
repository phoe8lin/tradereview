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


def _merge_orderflow(show: pd.DataFrame, orderflow: Optional[pd.DataFrame]) -> pd.DataFrame:
    """把 orderflow 的 delta/cvd 按 timestamp 合并到展示窗口。

    orderflow 由 tools.orderflow_fetcher 产出，窗口通常比 chart 显示窗口窄，
    超出窗口的 bar 会得到 NaN，绘图时自然留白。
    """
    if orderflow is None or orderflow.empty:
        return show
    keep_cols = [c for c in ("timestamp", "delta", "cvd") if c in orderflow.columns]
    if "timestamp" not in keep_cols or ("delta" not in keep_cols and "cvd" not in keep_cols):
        return show
    return show.merge(orderflow[keep_cols], on="timestamp", how="left")


def build_chart(
    df: pd.DataFrame,
    title: str,
    entry: Optional[float] = None,
    stop: Optional[float] = None,
    take: Optional[float] = None,
    direction: str = "long",
    output_html: str = "chart.html",
    label_every: int = 5,
    orderflow: Optional[pd.DataFrame] = None,
) -> str:
    """绘制并保存 HTML，返回文件路径。

    orderflow: 可选 DataFrame，至少含 timestamp + (delta 和/或 cvd) 列；
    存在且有效时图表会增加第三行 Delta/CVD 子图。
    """
    cfg = load_defaults()
    ema_cfg = cfg["ema"]
    chart_cfg = cfg["chart"]
    wf = cfg["wave_filter"]

    # 仅展示 in_display 窗口
    show = df[df["in_display"]].copy()
    show["hover"] = show.apply(_hover_text, axis=1)
    show = _merge_orderflow(show, orderflow)

    has_of = (
        ("delta" in show.columns and show["delta"].notna().any())
        or ("cvd" in show.columns and show["cvd"].notna().any())
    )

    if has_of:
        fig = make_subplots(
            rows=3,
            cols=1,
            shared_xaxes=True,
            row_heights=[0.60, 0.20, 0.20],
            vertical_spacing=0.025,
            subplot_titles=(title, "Wave Filter (StochRSI 变种)", "Delta / CVD"),
            specs=[[{}], [{}], [{"secondary_y": True}]],
        )
    else:
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

    # --- 入场 / 止损 / 止盈 + 盈亏区 ---
    # 标签放在图内左上角（避免之前 right 模式被右侧 margin 裁掉），
    # 右上角再用 paper 坐标放一张统一信息卡，集中显示 方向/R/RR。
    risk = abs(entry - stop) if (entry is not None and stop is not None) else None
    rr = round(abs(take - entry) / risk, 3) if (take is not None and risk) else None
    line_label_font = dict(size=11)
    if entry is not None:
        fig.add_hline(
            y=entry,
            line=dict(color=chart_cfg["entry_color"], width=1.5, dash="solid"),
            annotation_text=f"Entry {entry}",
            annotation_position="top left",
            annotation_font=dict(color=chart_cfg["entry_color"], **line_label_font),
            annotation_xshift=4,
            row=1,
            col=1,
        )
    if stop is not None:
        fig.add_hline(
            y=stop,
            line=dict(color=chart_cfg["stop_color"], width=1.5, dash="dash"),
            annotation_text=f"Stop {stop}",
            annotation_position="top left",
            annotation_font=dict(color=chart_cfg["stop_color"], **line_label_font),
            annotation_xshift=4,
            row=1,
            col=1,
        )
    if take is not None:
        fig.add_hline(
            y=take,
            line=dict(color=chart_cfg["take_color"], width=1.5, dash="dash"),
            annotation_text=f"Take {take}",
            annotation_position="top left",
            annotation_font=dict(color=chart_cfg["take_color"], **line_label_font),
            annotation_xshift=4,
            row=1,
            col=1,
        )

    # 右上角信息卡（paper 坐标，永不被裁切）
    info_lines = [f"<b>{direction.upper()}</b>"]
    if entry is not None:
        info_lines.append(f"Entry  <b>{entry}</b>")
    if stop is not None:
        line = f"Stop   <b>{stop}</b>"
        if risk is not None:
            line += f"   <span style='color:#888'>R={risk:.3f}</span>"
        info_lines.append(line)
    if take is not None:
        line = f"Take   <b>{take}</b>"
        if rr is not None:
            line += f"   <span style='color:#888'>RR={rr}</span>"
        info_lines.append(line)
    if len(info_lines) > 1:
        fig.add_annotation(
            xref="x domain", yref="y domain",
            x=1.0, y=1.0,
            xanchor="right", yanchor="top",
            text="<br>".join(info_lines),
            showarrow=False,
            align="left",
            font=dict(size=11, color="#333", family="monospace"),
            bgcolor="rgba(255,255,255,0.85)",
            bordercolor="#bbb",
            borderwidth=1,
            borderpad=6,
            row=1, col=1,
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

    # --- Delta / CVD 子图（按需） ---
    if has_of:
        # Delta 柱状（正绿负红）
        if "delta" in show.columns and show["delta"].notna().any():
            d = show["delta"]
            colors = np.where(
                d.fillna(0) >= 0,
                chart_cfg["candle_up_color"],
                chart_cfg["candle_down_color"],
            )
            fig.add_trace(
                go.Bar(
                    x=show["datetime"],
                    y=d,
                    marker=dict(color=colors),
                    name="Delta",
                    opacity=0.7,
                    hovertemplate="Δ %{y:.2f}<extra></extra>",
                ),
                row=3, col=1, secondary_y=False,
            )
            fig.add_hline(y=0, line=dict(color="#888", dash="dot", width=1), row=3, col=1)
        # CVD 折线（右轴）
        if "cvd" in show.columns and show["cvd"].notna().any():
            fig.add_trace(
                go.Scatter(
                    x=show["datetime"],
                    y=show["cvd"],
                    mode="lines",
                    line=dict(color="#5c6bc0", width=1.6),
                    name="CVD",
                    hovertemplate="CVD %{y:.2f}<extra></extra>",
                ),
                row=3, col=1, secondary_y=True,
            )
            fig.update_yaxes(title_text="CVD", row=3, col=1, secondary_y=True, showgrid=False)
        fig.update_yaxes(title_text="Δ", row=3, col=1, secondary_y=False)

    # --- Layout ---
    fig.update_layout(
        height=940 if has_of else 820,
        template="plotly_white",
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        legend=dict(orientation="h", y=1.02, x=0),
        margin=dict(l=40, r=30, t=50, b=30),
        bargap=0.15,
    )
    fig.update_xaxes(showspikes=True, spikethickness=1, spikedash="dot")
    fig.update_yaxes(showspikes=True, spikethickness=1, spikedash="dot")

    fig.write_html(output_html, include_plotlyjs="cdn")
    return output_html
