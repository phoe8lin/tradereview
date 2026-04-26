"""交易参数验证器：杜绝"凭印象写"的入场/止损/止盈。

输入一笔交易（entry_bar_id, side, entry, stop, take），逐项检查：
  A. entry 价合法性     —— entry ∈ {O,H,L,C} ?  否则视为限价单
  B. 限价单可达性       —— 后续 N 根 K 线是否触及 entry
  C. 止损位置合理性     —— 做空时 stop > entry_bar.high?  stop 与各 EMA 的关系
  D. RR 数学一致性     —— (entry-take)/(stop-entry) (做空) ≈ claimed_rr
  E. 后验兑现           —— stop / take 哪个先触发，多少根之后
  F. MAE / MFE          —— 持仓期间最坏未实现亏损 / 最大未实现盈利
  G. 擦边判定           —— take/stop 兑现是否 < 5 ticks 极限

输出 dict，所有 PASS/FAIL/WARN 都带具体数值证据。
"""
from __future__ import annotations

from typing import Dict, Optional

import pandas as pd

from .data_loader import get_bar, get_bars_after, parse_ref
from .price_ema import EMA_COLS

TICK = 0.001  # HYPE 5m 默认 tick；不同币种可参数化


def validate_trade(
    df: pd.DataFrame,
    entry_bar: str,
    side: str,                # 'short' or 'long'
    entry: float,
    stop: float,
    take: float,
    claimed_rr: Optional[float] = None,
    horizon: int = 30,
    tick: float = TICK,
    edge_ticks: int = 5,
) -> Dict:
    """对一笔交易做全面参数与后验校验。"""
    side = side.lower()
    assert side in ("short", "long"), "side 必须是 'short' 或 'long'"

    bar = get_bar(df, entry_bar)
    O, H, L, C = (float(bar[k]) for k in ["open", "high", "low", "close"])
    checks = {}

    # ---- A. entry 价合法性 ----
    ohlc = {"open": O, "high": H, "low": L, "close": C}
    matched = [k for k, v in ohlc.items() if abs(v - entry) < tick / 2]
    if matched:
        checks["A_entry_in_ohlc"] = {
            "status": "PASS",
            "matched": matched,
            "msg": f"entry={entry} 匹配 {matched}",
        }
    else:
        within_range = L - tick <= entry <= H + tick
        checks["A_entry_in_ohlc"] = {
            "status": "WARN" if within_range else "FAIL",
            "matched": [],
            "ohlc": {k: round(v, 4) for k, v in ohlc.items()},
            "msg": (
                f"entry={entry} 不是 OHLC 任何一个值 "
                f"(O={O} H={H} L={L} C={C})；"
                + ("在 K 线区间内，按限价单判定" if within_range else "在 K 线区间外，限价单不可能成交")
            ),
        }

    # ---- B. 限价单可达性 ----
    after = get_bars_after(df, entry_bar, n=horizon)
    if not matched:
        if side == "short":
            # 做空限价：等价格反弹到 entry 才挂上 → 后续 high 必须 >= entry
            reached = (after["high"] >= entry).any()
            max_h = float(after["high"].max()) if len(after) else None
            checks["B_reachability"] = {
                "status": "PASS" if reached else "FAIL",
                "msg": (
                    f"做空限价 {entry}: 后续 {horizon} 根最高 high={max_h} "
                    + ("已触及" if reached else f"未触及（差 {round(entry - max_h, 4)}）")
                ),
            }
        else:
            reached = (after["low"] <= entry).any()
            min_l = float(after["low"].min()) if len(after) else None
            checks["B_reachability"] = {
                "status": "PASS" if reached else "FAIL",
                "msg": (
                    f"做多限价 {entry}: 后续 {horizon} 根最低 low={min_l} "
                    + ("已触及" if reached else f"未触及（差 {round(min_l - entry, 4)}）")
                ),
            }
    else:
        checks["B_reachability"] = {"status": "N/A", "msg": "市价单（entry == OHLC 之一）"}

    # ---- C. 止损位置合理性 ----
    ema_info = {col: float(bar[col]) for col in EMA_COLS if col in bar and pd.notna(bar[col])}
    # 关键改进：too_wide 仅当 stop **跨越** entry 与某条 EMA 之间的边界
    # 即 entry 在 EMA 一侧、stop 在另一侧。
    if side == "short":
        stop_above_entry = stop > entry
        stop_above_bar_high = stop >= H
        crossed_emas = [
            k for k, v in ema_info.items()
            if entry < v <= stop  # entry 在 EMA 之下、stop 跨过 EMA 之上
        ]
        msg_parts = []
        msg_parts.append(f"stop {stop} {'>' if stop_above_entry else '<='} entry {entry}")
        msg_parts.append(
            f"stop vs K 线 high {H}: "
            + ("已覆盖高点" if stop_above_bar_high else "低于高点⚠️会被本根 K 扫损")
        )
        if crossed_emas:
            msg_parts.append(f"⚠️ 止损跨越 EMA: {crossed_emas}")
        else:
            msg_parts.append("止损未跨越任何 EMA")
        too_wide = "ema_200" in crossed_emas  # 跨越 ema_200 视为过宽
        status = "FAIL" if not stop_above_entry else (
            "WARN" if (not stop_above_bar_high or too_wide) else "PASS"
        )
        checks["C_stop_position"] = {
            "status": status,
            "stop_crossed_emas": crossed_emas,
            "msg": "; ".join(msg_parts),
        }
    else:
        stop_below_entry = stop < entry
        stop_below_bar_low = stop <= L
        crossed_emas = [
            k for k, v in ema_info.items()
            if stop <= v < entry  # entry 在 EMA 之上、stop 跨过 EMA 之下
        ]
        msg_parts = []
        msg_parts.append(f"stop {stop} {'<' if stop_below_entry else '>='} entry {entry}")
        msg_parts.append(
            f"stop vs K 线 low {L}: "
            + ("已覆盖低点" if stop_below_bar_low else "高于低点⚠️会被本根 K 扫损")
        )
        if crossed_emas:
            msg_parts.append(f"⚠️ 止损跨越 EMA: {crossed_emas}")
        else:
            msg_parts.append("止损未跨越任何 EMA")
        too_wide = "ema_200" in crossed_emas
        status = "FAIL" if not stop_below_entry else (
            "WARN" if (not stop_below_bar_low or too_wide) else "PASS"
        )
        checks["C_stop_position"] = {
            "status": status,
            "stop_crossed_emas": crossed_emas,
            "msg": "; ".join(msg_parts),
        }

    # ---- D. RR 数学一致性 ----
    if side == "short":
        risk = stop - entry
        reward = entry - take
    else:
        risk = entry - stop
        reward = take - entry
    actual_rr = round(reward / risk, 3) if risk > 0 else None
    rr_ok = (claimed_rr is None) or (
        actual_rr is not None and abs(actual_rr - claimed_rr) <= 0.05
    )
    if risk <= 0:
        rr_status = "FAIL"
        rr_msg = f"风险 {risk} ≤ 0：止损方向错误"
    elif reward <= 0:
        rr_status = "FAIL"
        rr_msg = f"收益 {reward} ≤ 0：止盈方向错误"
    else:
        rr_status = "PASS" if rr_ok else "FAIL"
        rr_msg = (
            f"R={round(risk, 4)} reward={round(reward, 4)} 实际 RR={actual_rr}"
            + (f"（声称 {claimed_rr}）" if claimed_rr is not None else "")
        )
    checks["D_rr_math"] = {"status": rr_status, "actual_rr": actual_rr, "msg": rr_msg}

    # ---- E. 后验兑现 ----
    fired_take_idx = None
    fired_stop_idx = None
    for i, row in after.iterrows():
        h, l = float(row["high"]), float(row["low"])
        if side == "short":
            if l <= take and fired_take_idx is None:
                fired_take_idx = i
            if h >= stop and fired_stop_idx is None:
                fired_stop_idx = i
        else:
            if h >= take and fired_take_idx is None:
                fired_take_idx = i
            if l <= stop and fired_stop_idx is None:
                fired_stop_idx = i
        if fired_take_idx is not None or fired_stop_idx is not None:
            break

    if fired_take_idx is not None and (
        fired_stop_idx is None or fired_take_idx <= fired_stop_idx
    ):
        outcome = "TAKE"
        bar_fire = df.loc[fired_take_idx]
    elif fired_stop_idx is not None:
        outcome = "STOP"
        bar_fire = df.loc[fired_stop_idx]
    else:
        outcome = "NEITHER"
        bar_fire = None

    if bar_fire is not None:
        bars_after_n = int(fired_take_idx if outcome == "TAKE" else fired_stop_idx) - parse_ref(df, entry_bar)
        checks["E_outcome"] = {
            "status": "PASS" if outcome == "TAKE" else ("FAIL" if outcome == "STOP" else "WARN"),
            "outcome": outcome,
            "fired_at": str(bar_fire["id"]),
            "fired_time": pd.to_datetime(bar_fire["datetime"]).strftime("%H:%M"),
            "bars_to_fire": bars_after_n,
            "fire_high": float(bar_fire["high"]),
            "fire_low": float(bar_fire["low"]),
            "msg": (
                f"{outcome} @ {bar_fire['id']} ({pd.to_datetime(bar_fire['datetime']).strftime('%H:%M')})"
                f"，{bars_after_n} 根后兑现 (H={float(bar_fire['high']):.3f} L={float(bar_fire['low']):.3f})"
            ),
        }
    else:
        checks["E_outcome"] = {
            "status": "WARN",
            "outcome": "NEITHER",
            "msg": f"后续 {horizon} 根内既未止盈也未止损",
        }

    # ---- F. MAE / MFE ----
    if len(after) > 0:
        if side == "short":
            mae = float(after["high"].max()) - entry  # 最坏向上偏移
            mfe = entry - float(after["low"].min())   # 最好向下偏移
        else:
            mae = entry - float(after["low"].min())
            mfe = float(after["high"].max()) - entry
        mae_r = round(mae / risk, 2) if risk > 0 else None
        mfe_r = round(mfe / risk, 2) if risk > 0 else None
        checks["F_mae_mfe"] = {
            "status": "INFO",
            "MAE": round(mae, 4),
            "MFE": round(mfe, 4),
            "MAE_vs_R": mae_r,
            "MFE_vs_R": mfe_r,
            "msg": f"MAE={round(mae, 4)}({mae_r}R), MFE={round(mfe, 4)}({mfe_r}R)",
        }

    # ---- G. 擦边判定 ----
    edge_threshold = edge_ticks * tick
    edge_notes = []
    if outcome == "TAKE" and bar_fire is not None:
        if side == "short":
            margin = take - float(bar_fire["low"])
            if 0 <= margin < edge_threshold:
                edge_notes.append(f"止盈擦边: 触及差 {round(margin, 4)} < {edge_ticks} ticks")
        else:
            margin = float(bar_fire["high"]) - take
            if 0 <= margin < edge_threshold:
                edge_notes.append(f"止盈擦边: 触及差 {round(margin, 4)} < {edge_ticks} ticks")
    # 持仓期间是否擦止损
    if outcome != "STOP" and len(after) > 0:
        if side == "short":
            closest = stop - float(after["high"].max())
            if 0 <= closest < edge_threshold:
                edge_notes.append(f"止损擦边: 最高距止损 {round(closest, 4)} < {edge_ticks} ticks")
        else:
            closest = float(after["low"].min()) - stop
            if 0 <= closest < edge_threshold:
                edge_notes.append(f"止损擦边: 最低距止损 {round(closest, 4)} < {edge_ticks} ticks")
    if edge_notes:
        checks["G_edge"] = {"status": "WARN", "notes": edge_notes, "msg": "; ".join(edge_notes)}
    else:
        checks["G_edge"] = {"status": "PASS", "notes": [], "msg": "无擦边"}

    # ---- 总评 ----
    statuses = [v.get("status") for v in checks.values()]
    overall = "FAIL" if "FAIL" in statuses else ("WARN" if "WARN" in statuses else "PASS")

    return {
        "trade": {
            "bar": entry_bar,
            "side": side,
            "entry": entry,
            "stop": stop,
            "take": take,
            "claimed_rr": claimed_rr,
        },
        "bar_ohlc": {"O": O, "H": H, "L": L, "C": C},
        "ema_at_entry": {k: round(v, 4) for k, v in ema_info.items()},
        "checks": checks,
        "overall": overall,
    }


def format_report(result: Dict) -> str:
    """把 validate_trade 的输出渲染成人类可读的多行报告。"""
    t = result["trade"]
    lines = []
    lines.append(
        f"=== TRADE {t['bar']} {t['side'].upper()} entry={t['entry']} "
        f"stop={t['stop']} take={t['take']}"
        + (f" claimedRR={t['claimed_rr']}" if t["claimed_rr"] else "")
        + " ==="
    )
    o = result["bar_ohlc"]
    lines.append(f"OHLC: O={o['O']} H={o['H']} L={o['L']} C={o['C']}")
    lines.append(f"EMA@entry: {result['ema_at_entry']}")
    for code, chk in result["checks"].items():
        st = chk.get("status", "?")
        marker = {"PASS": "✓", "WARN": "⚠", "FAIL": "✗", "INFO": "ℹ", "N/A": "-"}.get(st, "?")
        msg = chk.get("msg") or ""
        extra = {k: v for k, v in chk.items() if k not in ("status", "msg")}
        extra_str = (" | " + ", ".join(f"{k}={v}" for k, v in extra.items())) if extra else ""
        lines.append(f"  [{marker} {st}] {code}: {msg}{extra_str}")
    lines.append(f"OVERALL: {result['overall']}")
    return "\n".join(lines)
