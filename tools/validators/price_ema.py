"""价格-EMA 关系判定。

杜绝"被拒""接近""压制"等模糊表述，统一用结构化字段输出。

严格定义（针对一根 K 线 bar 与一条均线 ema）：
  - touched         : low <= ema <= high                （K 线穿越了 ema）
  - rejected_above  : touched 且 close < ema - eps       （做空向被拒，高点被压回）
  - rejected_below  : touched 且 close > ema + eps       （做多向被拒，低点被托起）
  - above_only      : low > ema                          （整根在 ema 之上，未触及）
  - below_only      : high < ema                         （整根在 ema 之下，未触及）

eps 默认 = 0（严格 close 在哪一侧），可传 tick 大小作 buffer。
"""
from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd

EMA_COLS = ["ema_21", "ema_55", "ema_100", "ema_200"]


def _classify(bar: pd.Series, ema_col: str, eps: float) -> str:
    ema = float(bar[ema_col])
    h, l, c = float(bar["high"]), float(bar["low"]), float(bar["close"])
    if l > ema:
        return "above_only"
    if h < ema:
        return "below_only"
    # touched
    if c < ema - eps:
        return "rejected_above"  # 高点被均线压回，做空向
    if c > ema + eps:
        return "rejected_below"  # 低点被均线托起，做多向
    return "touched_neutral"  # 触及但 close 落在 ema±eps 内


def ema_relation(
    bar: pd.Series,
    ema_cols: Optional[List[str]] = None,
    eps: float = 0.0,
) -> Dict:
    """对一根 K 线输出与所有 EMA 的结构化关系。

    返回示例：
    {
      'bar_id': 'B217', 'time': '18:05', 'O':..., 'H':..., 'L':..., 'C':...,
      'relation': {
          'ema_21':  {'value': 41.181, 'distance_close': +0.057, 'state': 'above_only'},
          'ema_55':  {'value': 41.218, 'distance_close': +0.020, 'state': 'above_only'},
          'ema_100': {'value': 41.255, 'distance_close': -0.017, 'state': 'below_only'},
          'ema_200': {'value': 41.286, 'distance_close': -0.048, 'state': 'below_only'},
      },
      'touched': [], 'rejected_above': [], 'rejected_below': [],
      'closest_above_close': ('ema_100', 0.017),     # close 上方最近的均线
      'closest_below_close': ('ema_55', 0.020),      # close 下方最近的均线
      'narrative': "B217 18:05 close=41.238: 在 ema21/ema55 上方, 在 ema100/ema200 下方; 未触及任何 EMA。"
    }
    """
    cols = ema_cols or EMA_COLS
    relation: Dict[str, Dict] = {}
    touched: List[str] = []
    rej_above: List[str] = []
    rej_below: List[str] = []

    c = float(bar["close"])
    for col in cols:
        if col not in bar or pd.isna(bar[col]):
            continue
        state = _classify(bar, col, eps)
        ema_val = float(bar[col])
        relation[col] = {
            "value": round(ema_val, 4),
            "distance_close": round(c - ema_val, 4),
            "state": state,
        }
        if state in ("touched_neutral", "rejected_above", "rejected_below"):
            touched.append(col)
        if state == "rejected_above":
            rej_above.append(col)
        if state == "rejected_below":
            rej_below.append(col)

    # 离 close 最近的、且在 close 上方/下方的均线（用于"上方阻力"/"下方支撑"）
    above = [(k, v["value"] - c) for k, v in relation.items() if v["value"] > c]
    below = [(k, c - v["value"]) for k, v in relation.items() if v["value"] < c]
    closest_above = min(above, key=lambda x: x[1]) if above else None
    closest_below = min(below, key=lambda x: x[1]) if below else None

    # 自动叙事（杜绝凭印象写错）
    above_emas = [k for k, v in relation.items() if v["value"] < c]  # close 在 ema 之上
    below_emas = [k for k, v in relation.items() if v["value"] > c]  # close 在 ema 之下
    bar_id = bar.get("id", "?")
    bar_t = pd.to_datetime(bar["datetime"]).strftime("%H:%M") if "datetime" in bar else "?"
    parts = []
    if above_emas:
        parts.append("close 在 " + "/".join(above_emas) + " 之上")
    if below_emas:
        parts.append("close 在 " + "/".join(below_emas) + " 之下")
    if rej_above:
        parts.append(f"上方被拒({'/'.join(rej_above)})")
    if rej_below:
        parts.append(f"下方被托({'/'.join(rej_below)})")
    if not touched:
        parts.append("未触及任何 EMA")
    narrative = f"{bar_id} {bar_t} close={c:.3f}: " + "; ".join(parts) + "."

    return {
        "bar_id": bar_id,
        "time": bar_t,
        "O": float(bar["open"]),
        "H": float(bar["high"]),
        "L": float(bar["low"]),
        "C": c,
        "relation": relation,
        "touched": touched,
        "rejected_above": rej_above,
        "rejected_below": rej_below,
        "closest_above_close": closest_above,
        "closest_below_close": closest_below,
        "narrative": narrative,
    }
