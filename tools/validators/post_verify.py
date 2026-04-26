"""逐根 K 线后验：输出持仓时间序列。

trade_params.validate_trade 给出聚合结论（哪根兑现 / MAE / MFE）；
本模块给出**逐根明细**，方便：
  - 画 unrealized_pnl 时间序列
  - 看 MAE/MFE 在哪根达到极值
  - 判断"擦边"和"反复"（多次接近止损 / 止盈）
"""
from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd

from .data_loader import get_bars_after, parse_ref


def post_verify(
    df: pd.DataFrame,
    entry_bar: str,
    side: str,
    entry: float,
    stop: float,
    take: float,
    horizon: int = 30,
) -> Dict:
    """逐根追踪一笔交易。

    返回:
      {
        'timeline': [
            {'id', 'time', 'high', 'low', 'close',
             'unrealized_R', 'mae_R_so_far', 'mfe_R_so_far',
             'dist_to_stop_R', 'dist_to_take_R',
             'stop_hit', 'take_hit', 'state'},
            ...
        ],
        'summary': {
            'outcome': 'TAKE'/'STOP'/'OPEN',
            'fired_at': 'B224' or None,
            'bars_to_fire': int or None,
            'max_mae_R': ..., 'max_mae_at': 'B?',
            'max_mfe_R': ..., 'max_mfe_at': 'B?',
            'min_dist_to_stop_R': ..., 'closest_stop_at': 'B?',
        }
      }

    单位均按 R = abs(stop - entry) 归一，便于跨标的比较。
    """
    side = side.lower()
    assert side in ("short", "long")

    risk = abs(stop - entry)
    if risk <= 0:
        raise ValueError(f"风险 R = {risk} 不正常")

    after = get_bars_after(df, entry_bar, n=horizon)
    timeline: List[Dict] = []

    fired = None  # ('TAKE'|'STOP', idx)
    mae_so_far = 0.0
    mfe_so_far = 0.0
    closest_stop = None  # (idx, dist_R)
    closest_take = None

    for i, row in after.iterrows():
        h, l, c = float(row["high"]), float(row["low"]), float(row["close"])

        if side == "short":
            # 未实现 R = (entry - close) / risk
            unreal = (entry - c) / risk
            # 当根 MAE candidate (high)
            mae_candidate = (h - entry) / risk        # 越正越糟
            mfe_candidate = (entry - l) / risk        # 越正越好
            stop_hit = h >= stop
            take_hit = l <= take
            dist_to_stop = (stop - h) / risk         # 越小越接近止损 (>=0 未触发)
            dist_to_take = (l - take) / risk         # 越小越接近止盈
        else:
            unreal = (c - entry) / risk
            mae_candidate = (entry - l) / risk
            mfe_candidate = (h - entry) / risk
            stop_hit = l <= stop
            take_hit = h >= take
            dist_to_stop = (l - stop) / risk
            dist_to_take = (take - h) / risk

        mae_so_far = max(mae_so_far, mae_candidate)
        mfe_so_far = max(mfe_so_far, mfe_candidate)

        if not stop_hit and (closest_stop is None or dist_to_stop < closest_stop[1]):
            closest_stop = (str(row["id"]), round(dist_to_stop, 3))
        if not take_hit and (closest_take is None or dist_to_take < closest_take[1]):
            closest_take = (str(row["id"]), round(dist_to_take, 3))

        # 决定本根结束态
        if fired is None:
            if take_hit and stop_hit:
                # 同根同时触发：保守视为 STOP（实盘多按时间细粒度，复盘看保守值）
                fired = ("AMBIGUOUS", i)
                state = "AMBIGUOUS"
            elif take_hit:
                fired = ("TAKE", i)
                state = "TAKE"
            elif stop_hit:
                fired = ("STOP", i)
                state = "STOP"
            else:
                state = "OPEN"
        else:
            state = "CLOSED"

        timeline.append({
            "id": str(row["id"]),
            "time": pd.to_datetime(row["datetime"]).strftime("%H:%M"),
            "high": round(h, 4),
            "low": round(l, 4),
            "close": round(c, 4),
            "unrealized_R": round(unreal, 3),
            "mae_R_so_far": round(mae_so_far, 3),
            "mfe_R_so_far": round(mfe_so_far, 3),
            "dist_to_stop_R": round(dist_to_stop, 3) if not stop_hit else 0.0,
            "dist_to_take_R": round(dist_to_take, 3) if not take_hit else 0.0,
            "stop_hit": bool(stop_hit),
            "take_hit": bool(take_hit),
            "state": state,
        })

        if fired is not None:
            break

    # 总结
    outcome = fired[0] if fired else "OPEN"
    fired_at = timeline[-1]["id"] if fired else None
    bars_to_fire = len(timeline) if fired else None

    # 极值定位
    max_mae = max((t["mae_R_so_far"] for t in timeline), default=0.0)
    max_mae_at = next((t["id"] for t in timeline if t["mae_R_so_far"] == max_mae), None)
    max_mfe = max((t["mfe_R_so_far"] for t in timeline), default=0.0)
    max_mfe_at = next((t["id"] for t in timeline if t["mfe_R_so_far"] == max_mfe), None)

    summary = {
        "outcome": outcome,
        "fired_at": fired_at,
        "bars_to_fire": bars_to_fire,
        "max_mae_R": round(max_mae, 3),
        "max_mae_at": max_mae_at,
        "max_mfe_R": round(max_mfe, 3),
        "max_mfe_at": max_mfe_at,
        "closest_stop": closest_stop,  # (id, dist_R)
        "closest_take": closest_take,
    }

    return {"timeline": timeline, "summary": summary}


def format_timeline(result: Dict, max_rows: Optional[int] = None) -> str:
    """渲染逐根明细为表格字符串。"""
    rows = result["timeline"]
    if max_rows is not None:
        rows = rows[:max_rows]
    lines = []
    lines.append(
        f"{'id':<6}{'time':<7}{'H':>8}{'L':>8}{'C':>8}"
        f"{'unrR':>7}{'maeR':>7}{'mfeR':>7}{'dstop':>7}{'dtake':>7} state"
    )
    for r in rows:
        lines.append(
            f"{r['id']:<6}{r['time']:<7}{r['high']:>8.3f}{r['low']:>8.3f}{r['close']:>8.3f}"
            f"{r['unrealized_R']:>7.2f}{r['mae_R_so_far']:>7.2f}{r['mfe_R_so_far']:>7.2f}"
            f"{r['dist_to_stop_R']:>7.2f}{r['dist_to_take_R']:>7.2f} {r['state']}"
        )
    s = result["summary"]
    lines.append("")
    lines.append(
        f"summary: outcome={s['outcome']} fired_at={s['fired_at']} "
        f"bars={s['bars_to_fire']} | maxMAE={s['max_mae_R']}R@{s['max_mae_at']} "
        f"maxMFE={s['max_mfe_R']}R@{s['max_mfe_at']}"
    )
    return "\n".join(lines)
