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
from .volume_profile import VolumeProfile


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


def _overlay_vp_lines_on_main(
    fig: "go.Figure",
    vp: Optional[VolumeProfile],
    *,
    row: int = 1,
    col: int = 1,
    hover_x: Optional[pd.Series] = None,
) -> None:
    """主图上的 VP 轻量提示：VA 浅紫底 + POC/VAH/VAL 三条水平线。

    不画直方图（直方图归属下方独立 VP 子图，避免主图拥挤）。
    若提供 ``hover_x``（show["datetime"]），会再叠三条**透明 scatter** 线，
    承载 POC/VAH/VAL 的概念解释 tooltip——鼠标悬停水平线位置即弹出说明。
    """
    if vp is None or vp.bins is None or vp.total_vol <= 0:
        return
    fig.add_shape(
        type="rect",
        xref="x domain", yref="y",
        x0=0.0, x1=1.0,
        y0=vp.va_low, y1=vp.va_high,
        fillcolor="rgba(154,114,194,0.06)",
        line=dict(width=0),
        layer="below",
        row=row, col=col,
    )
    fig.add_hline(y=vp.poc,
                  line=dict(color="#ff8c00", width=1.2, dash="solid"),
                  row=row, col=col)
    fig.add_hline(y=vp.va_high,
                  line=dict(color="#9a72c2", width=1, dash="dot"),
                  row=row, col=col)
    fig.add_hline(y=vp.va_low,
                  line=dict(color="#9a72c2", width=1, dash="dot"),
                  row=row, col=col)

    # hover tooltip 承载层：完全透明的 scatter 线
    if hover_x is None or len(hover_x) == 0:
        return
    hvn_txt = ("  ".join(f"{p:.4f}" for p in vp.hvn[:3])) if vp.hvn else "-"
    lvn_txt = ("  ".join(f"{p:.4f}" for p in vp.lvn[:3])) if vp.lvn else "-"
    tips = [
        (
            vp.poc, "#ff8c00",
            f"<b>POC {vp.poc:.4f}</b><br>"
            "Point of Control — 该窗口成交量最大的价位<br>"
            "通常作为磁吸位 / 公允价值中枢；价格远离 POC 后<br>"
            "常出现回抽试探。",
        ),
        (
            vp.va_high, "#9a72c2",
            f"<b>VAH {vp.va_high:.4f}</b><br>"
            "Value Area High — 价值区上沿（默认 70% 成交量）<br>"
            "上破多为趋势延续信号；带量回落则为假突破。",
        ),
        (
            vp.va_low, "#9a72c2",
            f"<b>VAL {vp.va_low:.4f}</b><br>"
            "Value Area Low — 价值区下沿（默认 70% 成交量）<br>"
            "下破多为趋势延续信号；带量收回则为假突破。<br>"
            f"<span style='color:#888'>HVN: {hvn_txt}<br>LVN: {lvn_txt}</span>",
        ),
    ]
    # 因主图使用 hovermode="x unified"，若用横跨全图的透明线作 hover 承载，
    # 会在每个 x 位置都把 POC/VAH/VAL 文案叠进统一 tooltip，过于嘈杂。
    # 改为在 x 轴右端附近放 3 个小标识点（橙/紫小菱形），只在最右端的 K 才出现，
    # 用户把鼠标移过去即弹出三行概念说明，不污染常规 K 线 hover。
    x_last = hover_x.iloc[-1]
    symbols = ["diamond", "triangle-up", "triangle-down"]
    for (y_val, color, text), sym in zip(tips, symbols):
        fig.add_trace(
            go.Scatter(
                x=[x_last],
                y=[y_val],
                mode="markers",
                marker=dict(symbol=sym, size=9, color=color,
                            line=dict(color="#fff", width=1)),
                hovertemplate=text + "<extra></extra>",
                showlegend=False,
                name="_vp_tip",
            ),
            row=row, col=col,
        )


def _render_vp_subplot(
    fig: "go.Figure",
    big_vp: Optional[VolumeProfile],
    micro_vps: Optional[list[VolumeProfile]],
    *,
    row: int,
    col: int = 1,
    big_vp_x_start: float = 1.005,
    big_vp_x_width: float = 0.13,
    tz_offset_ms: int = 0,
    yref_override: Optional[str] = None,
    xref_override: Optional[str] = None,
) -> None:
    """VP 综合子图：

    - 主区（plot area 内，x=time, y=price）：footprint 风格 micro-VP，
      每根 K 旁画一组迷你水平 bar，长度按 vol/max_vol_per_K 缩放，POC bin 用
      橙色强调。
    - 右侧 margin（x domain ∈ [big_vp_x_start, +big_vp_x_width]）：整窗大 VP
      横向直方图（可选）。
    """
    # ─── footprint micro-VP ───
    # 当 row 包含 secondary_y 子图（如 OF 行）会在 y 轴名上多占一个号，
    # 此时调用方需通过 xref_override / yref_override 显式给 VP 子图正确的 axis 名。
    if micro_vps:
        for mvp in micro_vps:
            ts = mvp.window.get("kline_ts")
            tf = mvp.window.get("kline_tf_ms", 0)
            if ts is None or tf <= 0 or mvp.total_vol <= 0:
                continue
            # bar 长度按 K 时间宽度 × (vol/max_vol)，居中对齐美观
            max_v = float(mvp.bins["vol"].max())
            if max_v <= 0:
                continue
            for _, b in mvp.bins.iterrows():
                v = float(b["vol"])
                if v <= 0:
                    continue
                ratio = v / max_v
                # 居中 bar：宽度 = tf * ratio
                bar_half = tf / 2 * ratio  # ms
                cx = ts + tf / 2
                x0 = pd.to_datetime(cx - bar_half + tz_offset_ms, unit="ms")
                x1 = pd.to_datetime(cx + bar_half + tz_offset_ms, unit="ms")
                is_poc = b["price_lo"] <= mvp.poc < b["price_hi"]
                fc = "rgba(255,140,0,0.70)" if is_poc else "rgba(120,140,180,0.55)"
                kwargs = dict(
                    type="rect",
                    x0=x0, x1=x1,
                    y0=float(b["price_lo"]), y1=float(b["price_hi"]),
                    fillcolor=fc,
                    line=dict(width=0),
                    layer="above",
                )
                if xref_override or yref_override:
                    kwargs["xref"] = xref_override or f"x{row}"
                    kwargs["yref"] = yref_override or f"y{row}"
                    fig.add_shape(**kwargs)
                else:
                    fig.add_shape(**kwargs, row=row, col=col)

    # ─── 整窗大 VP（右侧 margin 内）───
    if big_vp is not None and big_vp.total_vol > 0 and big_vp.bins is not None:
        # 解析 axis 名（考虑 secondary_y 错位）
        if xref_override or yref_override:
            xref_dom = (xref_override or f"x{row}") + " domain"
            yref_axis = yref_override or f"y{row}"
        else:
            xref_dom = "x domain"
            yref_axis = "y"

        max_vol = float(big_vp.bins["vol"].max())
        if max_vol > 0:
            x_left_anchor = big_vp_x_start
            for _, b in big_vp.bins.iterrows():
                v = float(b["vol"])
                if v <= 0:
                    continue
                ratio = v / max_vol
                x_right = x_left_anchor + big_vp_x_width * ratio
                is_poc = b["price_lo"] <= big_vp.poc < b["price_hi"]
                fc = "rgba(255,140,0,0.65)" if is_poc else "rgba(120,140,180,0.45)"
                kw = dict(
                    type="rect",
                    xref=xref_dom, yref=yref_axis,
                    x0=x_left_anchor, x1=x_right,
                    y0=float(b["price_lo"]), y1=float(b["price_hi"]),
                    fillcolor=fc,
                    line=dict(width=0),
                    layer="above",
                )
                if not (xref_override or yref_override):
                    fig.add_shape(**kw, row=row, col=col)
                else:
                    fig.add_shape(**kw)
            # baseline
            kw = dict(
                type="line",
                xref=xref_dom, yref=yref_axis,
                x0=x_left_anchor, x1=x_left_anchor,
                y0=float(big_vp.bins["price_lo"].iloc[0]),
                y1=float(big_vp.bins["price_hi"].iloc[-1]),
                line=dict(color="#aaa", width=1),
                layer="above",
            )
            if not (xref_override or yref_override):
                fig.add_shape(**kw, row=row, col=col)
            else:
                fig.add_shape(**kw)
        # 在 VP 子图也复制 POC/VAH/VAL 三条线
        for y_val, color in (
            (big_vp.poc,     "#ff8c00"),
            (big_vp.va_high, "#9a72c2"),
            (big_vp.va_low,  "#9a72c2"),
        ):
            kw = dict(
                type="line",
                xref=(xref_override or f"x{row}") + " domain"
                     if (xref_override or yref_override) else "x domain",
                yref=yref_axis,
                x0=0, x1=1, y0=y_val, y1=y_val,
                line=dict(color=color, width=1,
                          dash="solid" if color == "#ff8c00" else "dot"),
            )
            if not (xref_override or yref_override):
                fig.add_shape(**kw, row=row, col=col)
            else:
                fig.add_shape(**kw)


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
    big_vp: Optional[VolumeProfile] = None,
    micro_vps: Optional[list[VolumeProfile]] = None,
) -> str:
    """绘制并保存 HTML，返回文件路径。

    orderflow:  含 timestamp + (delta 和/或 cvd)；启用 Delta/CVD 子图。
    big_vp:     整窗 VP（OHLCV 近似或 trades 全量），用于主图浅紫底/POC/VAH/VAL
                + VP 子图右侧 margin 大直方图 + 信息卡。
    micro_vps:  锚 K 附近每根 K 一个的 micro-VP 列表，渲染到 VP 子图（footprint）。
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
    has_big_vp = big_vp is not None and big_vp.total_vol > 0
    has_micro = micro_vps is not None and len(micro_vps) > 0
    has_vp_section = has_big_vp or has_micro

    # 行索引：row 1 主图，row 2 wave，row 3 OF（如有），row N VP（如有，置最后）
    row_main, row_wave = 1, 2
    row_of = 3 if has_of else None
    row_vp = (2 + (1 if has_of else 0) + 1) if has_vp_section else None

    # 动态构建 subplots
    sub_titles = [title, "Wave Filter (StochRSI 变种)"]
    weights = [0.50, 0.16]
    specs: list = [[{}], [{}]]
    if has_of:
        sub_titles.append("Delta / CVD")
        weights.append(0.14)
        specs.append([{"secondary_y": True}])
    if has_vp_section:
        sub_titles.append("Volume Profile · footprint(锚±5) + 整窗大VP(右)")
        weights.append(0.20)
        specs.append([{}])
    total_w = sum(weights)
    row_heights = [w / total_w for w in weights]

    fig = make_subplots(
        rows=len(weights),
        cols=1,
        shared_xaxes=False,  # 不用 plotly 的 matches；显式控制各行 x range
        row_heights=row_heights,
        vertical_spacing=0.045,
        subplot_titles=tuple(sub_titles),
        specs=specs,
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
    if has_big_vp:
        vp = big_vp
        info_lines.append("<span style='color:#bbb'>──────────</span>")
        info_lines.append(
            f"<span style='color:#ff8c00'>POC</span>    <b>{vp.poc:.4f}</b>"
        )
        info_lines.append(
            f"<span style='color:#9a72c2'>VAH</span>    <b>{vp.va_high:.4f}</b>"
        )
        info_lines.append(
            f"<span style='color:#9a72c2'>VAL</span>    <b>{vp.va_low:.4f}</b>"
        )
        if vp.hvn:
            top_hvn = "  ".join(f"{p:.4f}" for p in vp.hvn[:3])
            info_lines.append(f"<span style='color:#888'>HVN</span>  {top_hvn}")
        info_lines.append(
            f"<span style='color:#888'>src={vp.source}</span>"
        )
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

    # --- Volume Profile：主图只画 VA 浅紫底 + POC/VAH/VAL 三条线（不画直方图） ---
    if has_big_vp:
        _overlay_vp_lines_on_main(
            fig, big_vp, row=row_main, col=1,
            hover_x=show["datetime"],
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

    # --- VP 综合子图（footprint micro-VP + 右 margin 大 VP） ---
    if has_vp_section:
        # 计算 timestamp(UTC epoch ms) → 显示用 naive datetime 的偏移
        # （df.datetime 列是 UTC+8 naive；df.timestamp 是 UTC epoch）
        first_dt = pd.Timestamp(show["datetime"].iloc[0])
        first_ts_utc = pd.to_datetime(int(show["timestamp"].iloc[0]), unit="ms")
        tz_offset_ms = int((first_dt - first_ts_utc).total_seconds() * 1000)

        def _ts_to_naive(ts_ms: int) -> pd.Timestamp:
            return pd.to_datetime(ts_ms + tz_offset_ms, unit="ms")

        # 1) 时间锚定：用所有 micro-VP 的中间价做隐形 scatter，
        #    既建立 x=date / y=price 类型，又让 plotly 自动推断 range。
        if has_micro:
            anchor_x = [
                _ts_to_naive(mvp.window["kline_ts"] + mvp.window.get("kline_tf_ms", 0) // 2)
                for mvp in micro_vps
            ]
            anchor_y = [mvp.poc for mvp in micro_vps]
            fig.add_trace(
                go.Scatter(
                    x=anchor_x, y=anchor_y, mode="markers",
                    marker=dict(color="rgba(0,0,0,0)", size=0.1),
                    showlegend=False, hoverinfo="skip", name="_vp_anchor",
                ),
                row=row_vp, col=1,
            )
        else:
            fig.add_trace(
                go.Scatter(
                    x=show["datetime"], y=show["close"],
                    mode="lines", line=dict(color="rgba(0,0,0,0)", width=0),
                    showlegend=False, hoverinfo="skip", name="_vp_anchor",
                ),
                row=row_vp, col=1,
            )

        # 2) 渲染 footprint + 右 margin 大 VP
        # 关键：当上方存在 secondary_y 子图（如 OF 行），plotly 给那行分两个
        # yaxis（y3 主 + y4 副），导致 row_vp=4 实际对应 yaxis5。这里显式
        # 算正确的 xref/yref，不依赖 plotly 从 row 推断（推断会落到 y4）。
        x_axis_idx = row_vp
        y_axis_idx = row_vp + (1 if has_of else 0)
        xref_vp = f"x{x_axis_idx}" if x_axis_idx > 1 else "x"
        yref_vp = f"y{y_axis_idx}" if y_axis_idx > 1 else "y"
        _render_vp_subplot(
            fig, big_vp, micro_vps,
            row=row_vp, col=1,
            tz_offset_ms=tz_offset_ms,
            xref_override=xref_vp,
            yref_override=yref_vp,
        )

        # 3) 显式设定 x 范围 = micro 时间跨度 ±10% padding（footprint 视角）
        if has_micro:
            ts_first = int(micro_vps[0].window["kline_ts"])
            tf = int(micro_vps[-1].window.get("kline_tf_ms", 0))
            ts_last = int(micro_vps[-1].window["kline_ts"]) + tf
            pad = max(int((ts_last - ts_first) * 0.10), tf)
            fig.update_xaxes(
                type="date",
                range=[_ts_to_naive(ts_first - pad), _ts_to_naive(ts_last + pad)],
                row=row_vp, col=1,
            )
        # 4) y 轴范围跟随 micro-VP 价格区间（更聚焦）
        if has_micro:
            y_lo = min(float(m.bins["price_lo"].iloc[0]) for m in micro_vps)
            y_hi = max(float(m.bins["price_hi"].iloc[-1]) for m in micro_vps)
            ypad = (y_hi - y_lo) * 0.05
            fig.update_yaxes(range=[y_lo - ypad, y_hi + ypad], row=row_vp, col=1)
        fig.update_yaxes(title_text="Price (VP)", row=row_vp, col=1)

    # --- Layout ---
    # 高度根据子图行数自适应
    base_h = 540
    base_h += 200 if has_of else 0
    base_h += 240 if has_vp_section else 0
    fig.update_layout(
        height=base_h,
        template="plotly_white",
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        legend=dict(orientation="h", y=1.02, x=0),
        # 大 VP 直方图外置在右侧 margin 时需要更宽的 r margin
        margin=dict(l=40, r=260 if has_vp_section else 30, t=50, b=30),
        bargap=0.15,
    )
    fig.update_xaxes(showspikes=True, spikethickness=1, spikedash="dot")
    fig.update_yaxes(showspikes=True, spikethickness=1, spikedash="dot")

    # 概念面板现在由 _inject_concept_panel() 在 write_html 之后以原生 HTML
    # <details> 方式注入到右上角，可点击折叠/展开（见文件末）。

    # 显式同步主图 / wave / OF 子图 x range（不用 matches，避免与 VP 子图打架）
    main_x_range = [show["datetime"].iloc[0], show["datetime"].iloc[-1]]
    fig.update_xaxes(type="date", range=main_x_range, row=row_main, col=1)
    fig.update_xaxes(type="date", range=main_x_range, row=row_wave, col=1)
    if row_of:
        fig.update_xaxes(type="date", range=main_x_range, row=row_of, col=1)
    # autosize：宽度跟随浏览器窗口；高度仍按 has_of 决定
    fig.update_layout(autosize=True)

    fig.write_html(
        output_html,
        include_plotlyjs="cdn",
        config={"responsive": True},
        default_width="100%",
    )
    if has_vp_section:
        _inject_concept_panel(output_html)
    return output_html


# ---------------------------------------------------------------------------
# 后处理：把 VP 概念速查面板以 <details> 形式注入 HTML，可折叠
# ---------------------------------------------------------------------------

_CONCEPT_PANEL_HTML = """
<style>
.vp-concept-panel{
  position:fixed; top:14px; right:14px; z-index:9999;
  max-width:240px;
  background:rgba(248,246,252,0.94);
  border:1px solid #9a72c2; border-radius:5px;
  font:11px/1.55 Menlo, Consolas, monospace; color:#333;
  box-shadow:0 2px 6px rgba(60,40,90,0.10);
}
.vp-concept-panel > summary{
  cursor:pointer; padding:6px 10px;
  font-weight:700; color:#6a4ca6;
  list-style:none; user-select:none;
}
.vp-concept-panel > summary::-webkit-details-marker{display:none}
.vp-concept-panel > summary::before{
  content:"\\25B8"; display:inline-block; width:10px;
  transition:transform .15s ease;
}
.vp-concept-panel[open] > summary::before{transform:rotate(90deg)}
.vp-concept-panel .body{padding:0 10px 8px 10px}
.vp-concept-panel hr{
  border:0; border-top:1px dashed #c7b8e0; margin:6px 0;
}
.vp-concept-panel b{color:#222}
</style>
<details class="vp-concept-panel">
  <summary>VP 概念速查</summary>
  <div class="body">
    <div><span style="color:#ff8c00">━</span> <b>POC</b> 成交量最大价位</div>
    <div><span style="color:#9a72c2">┄</span> <b>VAH/VAL</b> 价值区上/下沿 (70%量)</div>
    <div><b>HVN</b> 高量节点：支撑/阻力强</div>
    <div><b>LVN</b> 低量节点：易快速穿越</div>
    <hr/>
    <div><b>Footprint</b></div>
    <div>每根 K 旁迷你横向直方图</div>
    <div><span style="color:#ff8c00">橙</span>=该 K 的 POC bin</div>
    <div><span style="color:#788cb4">蓝灰</span>=普通量分布</div>
    <hr/>
    <div><b>右侧大 VP</b></div>
    <div>整窗聚合的横向直方图，</div>
    <div>用于辨识全段关键价位。</div>
  </div>
</details>
"""


def _inject_concept_panel(html_path: str) -> None:
    """把可折叠 VP 概念面板写入已生成的 plotly HTML。

    plotly 自身没有原生 collapsible 控件，最稳健的方案是在 ``write_html`` 之后
    把一段独立的 ``<details>`` + CSS 注入到 ``</body>`` 之前。**幂等**：若已注入
    则跳过；面板使用 ``position:fixed``，不影响 plotly 自身布局。
    """
    from pathlib import Path
    p = Path(html_path)
    try:
        txt = p.read_text(encoding="utf-8")
    except FileNotFoundError:
        return
    if "vp-concept-panel" in txt:
        return  # already injected
    if "</body>" in txt:
        txt = txt.replace("</body>", _CONCEPT_PANEL_HTML + "</body>", 1)
    else:
        txt += _CONCEPT_PANEL_HTML
    p.write_text(txt, encoding="utf-8")
