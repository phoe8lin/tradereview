"""K 线编号器：按显示窗口从左到右生成稳定编号（A001, A002, ...）。

- 编号只赋给 display 窗口内的 K 线，warmup 段不参与。
- 锚定 K 线会被单独标记为 anchor=True，便于绘图/复盘引用。
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

from .config import load_defaults


def assign_kline_index(
    df: pd.DataFrame,
    anchor_ms: int,
    display_before: Optional[int] = None,
    display_after: Optional[int] = None,
    prefix: Optional[str] = None,
    width: Optional[int] = None,
) -> pd.DataFrame:
    """为 df 新增 kline_id / in_display / is_anchor 三列。"""
    cfg = load_defaults()
    display_before = display_before if display_before is not None else cfg["data"]["display_bars_before"]
    display_after = display_after if display_after is not None else cfg["data"]["display_bars_after"]
    prefix = prefix or cfg["indexing"]["prefix"]
    width = width or cfg["indexing"]["width"]

    # 锚定 K 线位置
    tf_ms = int(df["timestamp"].diff().median())
    mask = (df["timestamp"] <= anchor_ms) & (anchor_ms < df["timestamp"] + tf_ms)
    if mask.any():
        anchor_idx = int(df.index[mask][0])
    else:
        anchor_idx = int((df["timestamp"] - anchor_ms).abs().idxmin())

    start_idx = max(0, anchor_idx - display_before)
    end_idx = min(len(df) - 1, anchor_idx + display_after)

    df["in_display"] = False
    df.loc[start_idx:end_idx, "in_display"] = True
    df["is_anchor"] = False
    df.loc[anchor_idx, "is_anchor"] = True

    # 以锚 K 为 A0，前为负数，后为正数
    ids = [""] * len(df)
    for i in range(start_idx, end_idx + 1):
        offset = i - anchor_idx
        if offset == 0:
            ids[i] = f"{prefix}0"
        elif offset > 0:
            ids[i] = f"{prefix}{offset}"
        else:
            ids[i] = f"{prefix}{offset}"  # offset 自带负号
    df["kline_id"] = ids

    df.attrs["anchor_idx"] = anchor_idx
    df.attrs["display_start_idx"] = start_idx
    df.attrs["display_end_idx"] = end_idx
    return df
