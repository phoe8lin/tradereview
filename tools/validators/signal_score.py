"""信号要素自动打分。

目前支持两个模板（与 2026-04-26 复盘提炼的"5 要素清单"一致）：

- ``short_reversal_overbought``：bull_stack/tangled 中超买回归做空
- ``long_reversal_oversold``：bear_stack/tangled 中超卖回归做多（对称）

每个模板返回：
  {
    'template': 'short_reversal_overbought',
    'items': [{'name', 'pass': bool, 'value', 'threshold', 'weight'}, ...],
    'score': 4,           # 命中数
    'total': 5,           # 总数
    'grade': 'B+',        # 由命中比例换算
    'narrative': '...'    # 自动叙事
  }

模板内部的阈值集中在 TEMPLATES dict，方便统一调参。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import pandas as pd

from .data_loader import get_bar, parse_ref


@dataclass
class CheckItem:
    name: str
    description: str
    passed: bool
    value: object
    threshold: object
    weight: int = 1

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "description": self.description,
            "pass": self.passed,
            "value": self.value,
            "threshold": self.threshold,
            "weight": self.weight,
        }


def _grade(score: int, total: int) -> str:
    """按命中比例换等级（与人工 review 中使用的等级体系一致）。"""
    if total == 0:
        return "?"
    ratio = score / total
    if ratio >= 1.0:
        return "A+"
    if ratio >= 0.8:
        return "B+"
    if ratio >= 0.6:
        return "B"
    if ratio >= 0.4:
        return "C"
    return "D"


# ============================================================
# Template: short_reversal_overbought
# ============================================================

def _check_short_reversal_overbought(
    df: pd.DataFrame,
    anchor_bar: pd.Series,
    entry_bar: pd.Series,
    th: Dict,
) -> List[CheckItem]:
    items: List[CheckItem] = []

    # 1. 锚 K 放量
    anchor_vol = float(anchor_bar["vol_vs_ma"])
    items.append(CheckItem(
        name="anchor_vol_vs_ma",
        description=f"锚 K vol_vs_ma >= {th['anchor_vol_min']}（放量推动）",
        passed=anchor_vol >= th["anchor_vol_min"],
        value=round(anchor_vol, 3),
        threshold=th["anchor_vol_min"],
    ))

    # 2. 入场 K vol 萎缩
    entry_vol = float(entry_bar["vol_vs_ma"])
    items.append(CheckItem(
        name="entry_vol_shrink",
        description=f"入场 K vol_vs_ma <= {th['entry_vol_max']}（萎缩衰竭）",
        passed=entry_vol <= th["entry_vol_max"],
        value=round(entry_vol, 3),
        threshold=th["entry_vol_max"],
    ))

    # 3. 入场 K delta 翻负
    entry_delta = float(entry_bar["delta"])
    items.append(CheckItem(
        name="entry_delta_negative",
        description="入场 K delta < 0（主动卖压翻转）",
        passed=entry_delta < 0,
        value=round(entry_delta, 1),
        threshold="< 0",
    ))

    # 4. CVD 较锚 K 下降
    cvd_drop = float(entry_bar["cvd"]) - float(anchor_bar["cvd"])
    items.append(CheckItem(
        name="cvd_top_turned",
        description="CVD 入场 K < 锚 K（顶部掉头）",
        passed=cvd_drop < 0,
        value=round(cvd_drop, 1),
        threshold="< 0",
    ))

    # 5. 距 ema_55 空间
    entry_close = float(entry_bar["close"])
    ema55 = float(entry_bar["ema_55"])
    distance_to_ema55 = entry_close - ema55  # 做空 entry 在 ema55 之上才有缓冲
    items.append(CheckItem(
        name="distance_to_ema55",
        description=f"close 距 ema_55 >= +{th['ema55_buffer']}（空间充足）",
        passed=distance_to_ema55 >= th["ema55_buffer"],
        value=round(distance_to_ema55, 4),
        threshold=f">= +{th['ema55_buffer']}",
    ))

    return items


# ============================================================
# Template: long_reversal_oversold (对称版本)
# ============================================================

def _check_long_reversal_oversold(
    df: pd.DataFrame,
    anchor_bar: pd.Series,
    entry_bar: pd.Series,
    th: Dict,
) -> List[CheckItem]:
    items: List[CheckItem] = []

    anchor_vol = float(anchor_bar["vol_vs_ma"])
    items.append(CheckItem(
        name="anchor_vol_vs_ma",
        description=f"锚 K vol_vs_ma >= {th['anchor_vol_min']}（放量砸盘）",
        passed=anchor_vol >= th["anchor_vol_min"],
        value=round(anchor_vol, 3),
        threshold=th["anchor_vol_min"],
    ))

    entry_vol = float(entry_bar["vol_vs_ma"])
    items.append(CheckItem(
        name="entry_vol_shrink",
        description=f"入场 K vol_vs_ma <= {th['entry_vol_max']}（萎缩衰竭）",
        passed=entry_vol <= th["entry_vol_max"],
        value=round(entry_vol, 3),
        threshold=th["entry_vol_max"],
    ))

    entry_delta = float(entry_bar["delta"])
    items.append(CheckItem(
        name="entry_delta_positive",
        description="入场 K delta > 0（主动买盘翻转）",
        passed=entry_delta > 0,
        value=round(entry_delta, 1),
        threshold="> 0",
    ))

    cvd_rise = float(entry_bar["cvd"]) - float(anchor_bar["cvd"])
    items.append(CheckItem(
        name="cvd_bottom_turned",
        description="CVD 入场 K > 锚 K（底部掉头）",
        passed=cvd_rise > 0,
        value=round(cvd_rise, 1),
        threshold="> 0",
    ))

    entry_close = float(entry_bar["close"])
    ema55 = float(entry_bar["ema_55"])
    distance_to_ema55 = ema55 - entry_close  # 做多 entry 在 ema55 之下才有缓冲
    items.append(CheckItem(
        name="distance_to_ema55",
        description=f"close 距 ema_55 >= +{th['ema55_buffer']}（下方空间）",
        passed=distance_to_ema55 >= th["ema55_buffer"],
        value=round(distance_to_ema55, 4),
        threshold=f">= +{th['ema55_buffer']}",
    ))

    return items


# ============================================================
# Template: short_kiss_ema21_in_bull_stack
# ----------------------------------------------------------------
# 适用场景：小窗口 (5m) `bull_stack` + 价格已连续若干根跌破 ema_21
#         + 当前 K 反弹回 kiss ema_21 from below + 反转印记
# anchor 与 entry 通常**同一根**（限价单单根成交）；不要求两根分立。
# ============================================================

def _check_short_kiss_ema21_in_bull_stack(
    df: pd.DataFrame,
    anchor_bar: pd.Series,
    entry_bar: pd.Series,
    th: Dict,
) -> List[CheckItem]:
    items: List[CheckItem] = []

    # 1. 入场 K 处于 bull_stack（小窗口仍是均线多头）
    stack = str(entry_bar.get("ema_stack", ""))
    items.append(CheckItem(
        name="regime_bull_stack",
        description="入场 K ema_stack == bull_stack（小窗口仍多头）",
        passed=stack == "bull_stack",
        value=stack,
        threshold="bull_stack",
    ))

    # 2. 前 lookback 根中至少 min_breakdown 根 close < ema_21（动能已转弱）
    lookback = int(th["lookback_bars"])
    min_break = int(th["min_breakdown_bars"])
    entry_idx = parse_ref(df, str(entry_bar["id"]))
    sub = df.iloc[max(0, entry_idx - lookback):entry_idx]
    breakdown_count = int((sub["close"] < sub["ema_21"]).sum()) if len(sub) else 0
    items.append(CheckItem(
        name="prior_breakdown_below_ema21",
        description=f"前 {lookback} 根中至少 {min_break} 根 close<ema_21（动能转弱）",
        passed=breakdown_count >= min_break,
        value=breakdown_count,
        threshold=f">= {min_break}",
    ))

    # 3. 入场 K 反抽并 kiss ema_21 from below
    high = float(entry_bar["high"])
    close = float(entry_bar["close"])
    ema21 = float(entry_bar["ema_21"])
    atr = float(entry_bar.get("atr", 0.0)) or 1.0
    kiss_tol = float(th["kiss_atr_tol"]) * atr
    kissed = (high >= ema21) and (abs(close - ema21) <= kiss_tol)
    items.append(CheckItem(
        name="entry_kiss_ema21",
        description=f"入场 K high>=ema_21 且 |close-ema_21|<= {th['kiss_atr_tol']}*ATR",
        passed=kissed,
        value=f"high={high:.3f},close={close:.3f},ema21={ema21:.3f},atr={atr:.3f}",
        threshold=f"|close-ema21|<= {th['kiss_atr_tol']}*ATR={kiss_tol:.3f}",
    ))

    # 4. 入场 K 主动卖压翻转（订单流确认）
    entry_delta = float(entry_bar.get("delta", 0.0))
    items.append(CheckItem(
        name="entry_delta_negative",
        description="入场 K delta < 0（反抽顶卖压介入）",
        passed=entry_delta < 0,
        value=round(entry_delta, 1),
        threshold="< 0",
    ))

    # 5. 入场 K buy_ratio 低于阈值（taker 卖方占优）
    buy_ratio = float(entry_bar.get("buy_ratio", 0.5))
    items.append(CheckItem(
        name="entry_buy_ratio_low",
        description=f"入场 K buy_ratio <= {th['buy_ratio_max']}（卖方主导）",
        passed=buy_ratio <= th["buy_ratio_max"],
        value=round(buy_ratio, 3),
        threshold=f"<= {th['buy_ratio_max']}",
    ))

    return items


# ============================================================
# Template: long_kiss_ema21_in_bear_stack（镜像）
# ============================================================

def _check_long_kiss_ema21_in_bear_stack(
    df: pd.DataFrame,
    anchor_bar: pd.Series,
    entry_bar: pd.Series,
    th: Dict,
) -> List[CheckItem]:
    items: List[CheckItem] = []

    stack = str(entry_bar.get("ema_stack", ""))
    items.append(CheckItem(
        name="regime_bear_stack",
        description="入场 K ema_stack == bear_stack（小窗口仍空头）",
        passed=stack == "bear_stack",
        value=stack,
        threshold="bear_stack",
    ))

    lookback = int(th["lookback_bars"])
    min_break = int(th["min_breakdown_bars"])
    entry_idx = parse_ref(df, str(entry_bar["id"]))
    sub = df.iloc[max(0, entry_idx - lookback):entry_idx]
    breakup_count = int((sub["close"] > sub["ema_21"]).sum()) if len(sub) else 0
    items.append(CheckItem(
        name="prior_breakup_above_ema21",
        description=f"前 {lookback} 根中至少 {min_break} 根 close>ema_21（动能转强）",
        passed=breakup_count >= min_break,
        value=breakup_count,
        threshold=f">= {min_break}",
    ))

    low = float(entry_bar["low"])
    close = float(entry_bar["close"])
    ema21 = float(entry_bar["ema_21"])
    atr = float(entry_bar.get("atr", 0.0)) or 1.0
    kiss_tol = float(th["kiss_atr_tol"]) * atr
    kissed = (low <= ema21) and (abs(close - ema21) <= kiss_tol)
    items.append(CheckItem(
        name="entry_kiss_ema21",
        description=f"入场 K low<=ema_21 且 |close-ema_21|<= {th['kiss_atr_tol']}*ATR",
        passed=kissed,
        value=f"low={low:.3f},close={close:.3f},ema21={ema21:.3f},atr={atr:.3f}",
        threshold=f"|close-ema21|<= {th['kiss_atr_tol']}*ATR={kiss_tol:.3f}",
    ))

    entry_delta = float(entry_bar.get("delta", 0.0))
    items.append(CheckItem(
        name="entry_delta_positive",
        description="入场 K delta > 0（回踩底买盘介入）",
        passed=entry_delta > 0,
        value=round(entry_delta, 1),
        threshold="> 0",
    ))

    buy_ratio = float(entry_bar.get("buy_ratio", 0.5))
    items.append(CheckItem(
        name="entry_buy_ratio_high",
        description=f"入场 K buy_ratio >= {th['buy_ratio_min']}（买方主导）",
        passed=buy_ratio >= th["buy_ratio_min"],
        value=round(buy_ratio, 3),
        threshold=f">= {th['buy_ratio_min']}",
    ))

    return items


# ============================================================
# Templates registry
# ============================================================

TEMPLATES: Dict[str, Dict] = {
    "short_reversal_overbought": {
        "checker": _check_short_reversal_overbought,
        "thresholds": {
            "anchor_vol_min": 1.8,
            "entry_vol_max": 0.6,
            "ema55_buffer": 0.06,
        },
    },
    "long_reversal_oversold": {
        "checker": _check_long_reversal_oversold,
        "thresholds": {
            "anchor_vol_min": 1.8,
            "entry_vol_max": 0.6,
            "ema55_buffer": 0.06,
        },
    },
    "short_kiss_ema21_in_bull_stack": {
        "checker": _check_short_kiss_ema21_in_bull_stack,
        "thresholds": {
            "lookback_bars": 8,
            "min_breakdown_bars": 4,
            "kiss_atr_tol": 0.4,    # close 距 ema_21 容差 = 0.4 * ATR
            "buy_ratio_max": 0.45,
        },
    },
    "long_kiss_ema21_in_bear_stack": {
        "checker": _check_long_kiss_ema21_in_bear_stack,
        "thresholds": {
            "lookback_bars": 8,
            "min_breakdown_bars": 4,
            "kiss_atr_tol": 0.4,
            "buy_ratio_min": 0.55,
        },
    },
}


def list_templates() -> List[str]:
    return list(TEMPLATES.keys())


def score_signal(
    df: pd.DataFrame,
    anchor: str,
    entry: str,
    template: str = "short_reversal_overbought",
    thresholds_override: Optional[Dict] = None,
) -> Dict:
    """对一对 (anchor, entry) 按模板打分。

    anchor / entry: 接受 'B193' / '16:05' / int / Timestamp（同 parse_ref）
    """
    if template not in TEMPLATES:
        raise KeyError(f"未知模板 {template}; 可用: {list_templates()}")
    cfg = TEMPLATES[template]
    th = dict(cfg["thresholds"])
    if thresholds_override:
        th.update(thresholds_override)

    anchor_bar = get_bar(df, anchor)
    entry_bar = get_bar(df, entry)

    items = cfg["checker"](df, anchor_bar, entry_bar, th)
    score = sum(1 for it in items if it.passed)
    total = len(items)
    grade = _grade(score, total)

    # 自动叙事
    pass_names = [it.name for it in items if it.passed]
    fail_names = [it.name for it in items if not it.passed]
    narr = (
        f"[{template}] anchor={anchor_bar['id']} entry={entry_bar['id']}: "
        f"{score}/{total} ({grade})"
    )
    if pass_names:
        narr += f" | ✓ {','.join(pass_names)}"
    if fail_names:
        narr += f" | ✗ {','.join(fail_names)}"

    return {
        "template": template,
        "anchor": str(anchor_bar["id"]),
        "entry": str(entry_bar["id"]),
        "items": [it.to_dict() for it in items],
        "score": score,
        "total": total,
        "grade": grade,
        "narrative": narr,
        "thresholds": th,
    }


def format_score_report(result: Dict) -> str:
    """渲染人类可读报告。"""
    lines = [result["narrative"]]
    for it in result["items"]:
        marker = "✓" if it["pass"] else "✗"
        lines.append(
            f"  [{marker}] {it['name']}: value={it['value']} threshold={it['threshold']}"
            f"  ({it['description']})"
        )
    return "\n".join(lines)
