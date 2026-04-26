"""EMA 排列状态时段分析。

杜绝"今天是 bear 趋势日"这种忽略时段的笼统判定。

核心函数：

- ``regime_segments(df)`` 把全日切成连续 ema_stack 段
- ``regime_at(df, ref)`` 查任意 K 所在段
- ``transitions(df)`` 列出所有切换点
- ``regime_summary(df)`` 统计各 stack 累计根数与占比

ema_stack 取值（来源 parquet）：
- ``bull_stack``  : ema21 > ema55 > ema100 > ema200 多头排列
- ``bear_stack``  : ema21 < ema55 < ema100 < ema200 空头排列
- ``tangled``     : 其他（缠绕/转换）
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Union

import pandas as pd

from .data_loader import parse_ref


@dataclass
class RegimeSegment:
    stack: str
    start_id: str
    end_id: str
    start_time: str
    end_time: str
    n_bars: int

    def to_dict(self) -> Dict:
        return {
            "stack": self.stack,
            "start_id": self.start_id,
            "end_id": self.end_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "n_bars": self.n_bars,
        }


def regime_segments(df: pd.DataFrame) -> List[RegimeSegment]:
    """切成连续 ema_stack 段。"""
    if "ema_stack" not in df.columns:
        raise KeyError("df 缺少 ema_stack 列")

    segs: List[RegimeSegment] = []
    if len(df) == 0:
        return segs

    cur_stack = df.iloc[0]["ema_stack"]
    cur_start = 0
    for i in range(1, len(df)):
        if df.iloc[i]["ema_stack"] != cur_stack:
            row_start, row_end = df.iloc[cur_start], df.iloc[i - 1]
            segs.append(RegimeSegment(
                stack=str(cur_stack),
                start_id=str(row_start["id"]),
                end_id=str(row_end["id"]),
                start_time=pd.to_datetime(row_start["datetime"]).strftime("%H:%M"),
                end_time=pd.to_datetime(row_end["datetime"]).strftime("%H:%M"),
                n_bars=i - cur_start,
            ))
            cur_stack = df.iloc[i]["ema_stack"]
            cur_start = i

    # 收尾段
    row_start, row_end = df.iloc[cur_start], df.iloc[-1]
    segs.append(RegimeSegment(
        stack=str(cur_stack),
        start_id=str(row_start["id"]),
        end_id=str(row_end["id"]),
        start_time=pd.to_datetime(row_start["datetime"]).strftime("%H:%M"),
        end_time=pd.to_datetime(row_end["datetime"]).strftime("%H:%M"),
        n_bars=len(df) - cur_start,
    ))
    return segs


def regime_at(df: pd.DataFrame, ref) -> RegimeSegment:
    """查某根 K 所在的 regime 段。"""
    idx = parse_ref(df, ref)
    bar_id = str(df.iloc[idx]["id"])
    for seg in regime_segments(df):
        if seg.start_id <= bar_id <= seg.end_id:
            # 字符串比较对 'B7' < 'B70' 不安全，用数字
            pass
    # 用数值索引比较更稳
    segs = regime_segments(df)
    for seg in segs:
        i0 = parse_ref(df, seg.start_id)
        i1 = parse_ref(df, seg.end_id)
        if i0 <= idx <= i1:
            return seg
    raise RuntimeError(f"未找到 {ref} 所在 regime 段")


def transitions(df: pd.DataFrame) -> List[Dict]:
    """所有切换点：[(from_stack, to_stack, at_id, at_time), ...]"""
    segs = regime_segments(df)
    out: List[Dict] = []
    for prev, curr in zip(segs[:-1], segs[1:]):
        out.append({
            "from": prev.stack,
            "to": curr.stack,
            "at_id": curr.start_id,
            "at_time": curr.start_time,
        })
    return out


def regime_summary(df: pd.DataFrame) -> Dict:
    """累计统计：各 stack 总根数、占比、最长段。"""
    segs = regime_segments(df)
    total = sum(s.n_bars for s in segs)
    by_stack: Dict[str, Dict] = {}
    for s in segs:
        d = by_stack.setdefault(s.stack, {"n_bars": 0, "n_segments": 0, "longest": 0, "longest_range": None})
        d["n_bars"] += s.n_bars
        d["n_segments"] += 1
        if s.n_bars > d["longest"]:
            d["longest"] = s.n_bars
            d["longest_range"] = f"{s.start_id}-{s.end_id} ({s.start_time}-{s.end_time})"
    for k, d in by_stack.items():
        d["pct"] = round(d["n_bars"] / total * 100, 1) if total > 0 else 0.0

    # 主导 stack
    dominant = max(by_stack.items(), key=lambda kv: kv[1]["n_bars"])[0] if by_stack else None
    dominant_pct = by_stack[dominant]["pct"] if dominant else 0.0

    return {
        "total_bars": total,
        "by_stack": by_stack,
        "dominant_stack": dominant,
        "dominant_pct": dominant_pct,
        "is_trend_day": dominant_pct >= 60.0,  # >=60% 算趋势日
        "n_transitions": len(segs) - 1,
    }


def format_segments(df: pd.DataFrame) -> str:
    """渲染人类可读时段表。"""
    segs = regime_segments(df)
    summary = regime_summary(df)
    lines = []
    lines.append(f"# EMA regime — {len(segs)} 段, {summary['n_transitions']} 次切换")
    lines.append(f"  主导 stack: {summary['dominant_stack']} ({summary['dominant_pct']}%)"
                 f" → {'趋势日' if summary['is_trend_day'] else '震荡/混合日'}")
    lines.append("")
    lines.append(f"  {'#':>3}  {'stack':<12}{'起':<6}{'止':<6}{'起时':<6}{'止时':<6}  根数")
    for i, s in enumerate(segs, 1):
        lines.append(
            f"  {i:>3}  {s.stack:<12}{s.start_id:<6}{s.end_id:<6}"
            f"{s.start_time:<6}{s.end_time:<6}  {s.n_bars}"
        )
    return "\n".join(lines)
