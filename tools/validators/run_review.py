"""一键复盘：从 trades.yaml 读取交易，调用所有校验器，输出 markdown。

trades.yaml 格式::

    date: 2026-04-26
    symbol: HYPE
    timeframe: 5m
    trades:
      - id: B193                # 入场 K
        side: short
        entry: 41.242
        stop: 41.336
        take: 41.112
        rr: 1.38                # 可选，会用于一致性校验
        template: short_reversal_overbought   # 可选，不填则跳过 signal_score
        anchor: B192            # template != None 时必填
        notes: "首仓"

用法::

    python -m tools.validators.run_review reviews/2026-04-26/trades_validate.yaml \
        > reviews/2026-04-26/auto_validation.md
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List

import yaml

from .data_loader import load_day, get_bar
from .price_ema import ema_relation
from .signal_score import score_signal
from .trade_params import validate_trade
from .bar_features import detect_features
from .ema_regime import regime_at, regime_summary, format_segments


def _md_status(s: str) -> str:
    return {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌", "INFO": "ℹ️", "N/A": "—"}.get(s, "❓")


def render_trade_section(df, t: Dict) -> str:
    """单笔交易的 markdown 段落。"""
    out: List[str] = []
    tid = t["id"]
    side = t["side"]
    entry, stop, take = t["entry"], t["stop"], t["take"]
    rr = t.get("rr")

    bar = get_bar(df, tid)
    out.append(f"### {tid} ({bar['datetime'].strftime('%H:%M')}) {side.upper()} "
               f"E={entry} / S={stop} / T={take}"
               + (f" / RR={rr}" if rr else ""))
    if t.get("notes"):
        out.append(f"> {t['notes']}")
    out.append("")

    # 入场 K 所在 EMA regime 段
    seg = regime_at(df, tid)
    out.append(f"**所处 EMA regime**: `{seg.stack}` 段 "
               f"({seg.start_id} {seg.start_time} → {seg.end_id} {seg.end_time}, {seg.n_bars} 根)")

    # 锚 K 信息（若提供 anchor）
    if "anchor" in t:
        anchor_bar = get_bar(df, t["anchor"])
        anchor_feat = detect_features(anchor_bar)
        out.append(f"**锚 K**: `{t['anchor']}` ({anchor_bar['datetime'].strftime('%H:%M')}) "
                   f"O={anchor_bar['open']:.3f} H={anchor_bar['high']:.3f} "
                   f"L={anchor_bar['low']:.3f} C={anchor_bar['close']:.3f} "
                   f"wave={anchor_bar['wave']:.2f} vol={anchor_bar['vol_vs_ma']:.2f} "
                   f"delta={anchor_bar['delta']:.0f} cvd={anchor_bar['cvd']:.0f}")
        if anchor_feat.patterns:
            out.append(f"**锚 K 形态**: {', '.join(anchor_feat.patterns)}")

    entry_feat = detect_features(bar)
    out.append(f"**入场 K**: O={bar['open']:.3f} H={bar['high']:.3f} "
               f"L={bar['low']:.3f} C={bar['close']:.3f} "
               f"wave={bar['wave']:.2f} vol={bar['vol_vs_ma']:.2f} "
               f"delta={bar['delta']:.0f} cvd={bar['cvd']:.0f}")
    if entry_feat.patterns:
        out.append(f"**入场 K 形态**: {', '.join(entry_feat.patterns)}")
    out.append("")

    # 价格-EMA 关系
    rel = ema_relation(bar)
    out.append("**入场 K 与 EMA 关系**: " + rel["narrative"])
    out.append("")

    # 信号要素打分
    if t.get("template"):
        if "anchor" not in t:
            out.append(f"⚠️ template={t['template']} 但未提供 anchor，跳过打分。")
        else:
            sig = score_signal(df, t["anchor"], tid, template=t["template"])
            out.append(f"**信号要素打分（{t['template']}）: {sig['score']}/{sig['total']} → {sig['grade']}**\n")
            out.append("| 要素 | 实测 | 阈值 | 通过 |")
            out.append("|---|---|---|---|")
            for it in sig["items"]:
                ok = "✅" if it["pass"] else "❌"
                out.append(f"| {it['name']} | {it['value']} | {it['threshold']} | {ok} |")
            out.append("")

    # 交易参数验证
    vr = validate_trade(df, tid, side, entry, stop, take, claimed_rr=rr)
    out.append(f"**参数与后验校验（overall: {_md_status(vr['overall'])} {vr['overall']}）**\n")
    out.append("| 项 | 状态 | 说明 |")
    out.append("|---|---|---|")
    for code, chk in vr["checks"].items():
        st = chk.get("status", "?")
        msg = chk.get("msg", "")
        # 截断过长 msg
        if len(msg) > 140:
            msg = msg[:137] + "..."
        msg = msg.replace("|", "\\|")
        out.append(f"| {code} | {_md_status(st)} {st} | {msg} |")
    out.append("")

    return "\n".join(out)


def render_review(yaml_path: str | Path) -> str:
    """从 yaml 渲染整份 markdown。"""
    yaml_path = Path(yaml_path)
    cfg = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))

    date = str(cfg["date"])  # yaml 可能解析成 datetime.date
    symbol = cfg.get("symbol", "HYPE")
    tf = cfg.get("timeframe", "5m")
    df = load_day(date, symbol, tf)

    out = []
    out.append(f"# 自动校验报告 — {date} {symbol} {tf}")
    out.append(f"> 生成自 `{yaml_path.name}`，数据源 `{df.attrs.get('source_path', '?')}`")
    out.append(f"> 共 {len(cfg['trades'])} 笔交易")
    out.append("")

    # 全日 EMA regime 概览
    rs = regime_summary(df)
    day_kind = "趋势日" if rs["is_trend_day"] else "震荡/混合日"
    out.append("## 全日 EMA regime")
    out.append(f"主导 stack: **{rs['dominant_stack']}** ({rs['dominant_pct']}%) → **{day_kind}**, "
               f"共 {rs['n_transitions']} 次切换")
    out.append("")
    out.append("```")
    out.append(format_segments(df))
    out.append("```")
    out.append("")

    # 顶部汇总表
    out.append("## 汇总")
    out.append("| # | id | 时间 | side | E / S / T | claimedRR | overall | signal |")
    out.append("|---|---|---|---|---|---|---|---|")
    summary_rows = []
    for i, t in enumerate(cfg["trades"], 1):
        bar = get_bar(df, t["id"])
        vr = validate_trade(df, t["id"], t["side"], t["entry"], t["stop"], t["take"],
                            claimed_rr=t.get("rr"))
        sig_str = "—"
        if t.get("template") and "anchor" in t:
            sig = score_signal(df, t["anchor"], t["id"], template=t["template"])
            sig_str = f"{sig['score']}/{sig['total']} {sig['grade']}"
        out.append(
            f"| {i} | {t['id']} | {bar['datetime'].strftime('%H:%M')} | {t['side']} | "
            f"{t['entry']}/{t['stop']}/{t['take']} | {t.get('rr', '—')} | "
            f"{_md_status(vr['overall'])} {vr['overall']} | {sig_str} |"
        )
        summary_rows.append((t["id"], vr["overall"]))
    out.append("")

    # 逐笔详情
    out.append("## 逐笔详情")
    out.append("")
    for t in cfg["trades"]:
        out.append(render_trade_section(df, t))
        out.append("---")
        out.append("")

    return "\n".join(out)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    print(render_review(sys.argv[1]))


if __name__ == "__main__":
    main()
