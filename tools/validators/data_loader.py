"""统一数据入口 —— 所有复盘脚本/笔记必须从这里取数。

设计要点：
- parquet 的 datetime 列**就是 UTC+8**，不做任何时区偏移
- K 线 id 与时间双向可查
- 引用一律用 parse_ref：可接受 'B193' / '16:05' / Timestamp / int
"""
from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_day(date: str, symbol: str = "HYPE", tf: str = "5m") -> pd.DataFrame:
    """加载某日某币种的复盘 parquet。

    路径规则: reviews/{date}/data/day_{symbol}_{tf}.parquet
    返回的 DataFrame 保证：
      - datetime 列为 UTC+8 naive timestamp
      - 包含 id 列（如 B00..B230）
      - 索引按 timestamp 升序
    """
    path = PROJECT_ROOT / "reviews" / date / "data" / f"day_{symbol}_{tf}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"未找到日内数据文件: {path}")
    df = pd.read_parquet(path)
    df = df.sort_values("timestamp").reset_index(drop=True)
    # datetime 一律视为 UTC+8 naive
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.attrs["date"] = date
    df.attrs["symbol"] = symbol
    df.attrs["tf"] = tf
    df.attrs["source_path"] = str(path)
    return df


def parse_ref(df: pd.DataFrame, ref: Union[str, int, pd.Timestamp]) -> int:
    """把多种形式的引用统一映射为 df 的行索引 (iloc)。

    支持：
      - 'B193' / 'B07' / 'B7'      (id 直接匹配，零填充不敏感)
      - '16:05' / '09:40'          (UTC+8 HH:MM)
      - int                        (直接当行索引)
      - pd.Timestamp               (与 datetime 列匹配)
    """
    if isinstance(ref, int):
        if ref < 0 or ref >= len(df):
            raise IndexError(f"行索引越界: {ref}")
        return ref

    if isinstance(ref, pd.Timestamp):
        match = df.index[df["datetime"] == ref]
        if len(match) == 0:
            raise KeyError(f"未找到时间 {ref}")
        return int(match[0])

    if not isinstance(ref, str):
        raise TypeError(f"不支持的 ref 类型: {type(ref)}")

    s = ref.strip()
    # 形如 'B193' / 'B07'
    if s.upper().startswith("B"):
        target_n = int(s[1:])
        # 兼容零填充和非零填充
        ids = df["id"].astype(str).str.upper()
        for cand in (f"B{target_n}", f"B{target_n:02d}", f"B{target_n:03d}"):
            mask = ids == cand
            if mask.any():
                return int(df.index[mask][0])
        raise KeyError(f"未找到 K 线 id: {ref}")

    # 形如 'HH:MM'
    if ":" in s:
        hhmm = s
        match = df.index[df["datetime"].dt.strftime("%H:%M") == hhmm]
        if len(match) == 0:
            raise KeyError(f"未找到 UTC+8 时间 {hhmm}")
        return int(match[0])

    raise ValueError(f"无法解析引用: {ref!r}")


def get_bar(df: pd.DataFrame, ref: Union[str, int, pd.Timestamp]) -> pd.Series:
    """取一根 K 线（pandas Series），ref 接受 id / 'HH:MM' / int / Timestamp。"""
    idx = parse_ref(df, ref)
    return df.iloc[idx].copy()


def get_bars_after(df: pd.DataFrame, ref, n: int = 30) -> pd.DataFrame:
    """取某根 K 之后的 n 根 K 线（不含 ref 本身）。"""
    idx = parse_ref(df, ref)
    end = min(idx + 1 + n, len(df))
    return df.iloc[idx + 1:end].copy()
