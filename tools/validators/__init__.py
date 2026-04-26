"""复盘流程化校验工具集 (P1).

固化以下铁律：
1. parquet 的 datetime 列已是 UTC+8，禁止再做时区偏移
2. K 线 id 形如 B{n}（不补零或两位补零均接受），按 5m TF 对应当日第 n 根
3. 所有数值（OHLC/EMA/wave/delta/CVD）一律从 parquet 直接取，禁止凭印象写
"""
from .data_loader import load_day, get_bar, parse_ref
from .price_ema import ema_relation
from .trade_params import validate_trade
from .signal_score import score_signal, list_templates, TEMPLATES
from .post_verify import post_verify
from .bar_features import detect_features, scan_features, DEFAULT_THRESHOLDS
from .ema_regime import (
    regime_segments,
    regime_at,
    transitions,
    regime_summary,
)

__all__ = [
    "load_day",
    "get_bar",
    "parse_ref",
    "ema_relation",
    "validate_trade",
    "score_signal",
    "list_templates",
    "TEMPLATES",
    "post_verify",
    "detect_features",
    "scan_features",
    "DEFAULT_THRESHOLDS",
    "regime_segments",
    "regime_at",
    "transitions",
    "regime_summary",
]
