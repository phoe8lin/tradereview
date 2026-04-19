"""功能性配置：代理、路径、Python 解释器等"""
from __future__ import annotations

import os
from pathlib import Path

import yaml

# ---- 路径 ----
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "defaults.yaml"
REVIEWS_DIR = PROJECT_ROOT / "reviews"
INDEX_DIR = PROJECT_ROOT / "index"

# ---- 代理 ----
PROXY = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890",
}

# ---- Python 解释器（提示性常量，实际由 shebang/环境决定） ----
PYTHON_BIN = "/opt/anaconda3/envs/trade/bin/python"


def load_defaults() -> dict:
    """加载业务默认配置"""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_dirs() -> None:
    REVIEWS_DIR.mkdir(parents=True, exist_ok=True)
    INDEX_DIR.mkdir(parents=True, exist_ok=True)
