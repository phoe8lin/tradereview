"""实时价格监控 + 条件信号检测。

给定标的/周期/一组条件，拉取最新 K 线，计算指标，检测是否触发。

典型用法：

    # 单次检查
    /opt/anaconda3/envs/trade/bin/python -m tools.monitor \
        --base HYPE --timeframe 5m --exchange binance --market futures

    # 持续监控（每 30 秒检查一次）
    /opt/anaconda3/envs/trade/bin/python -m tools.monitor \
        --base HYPE --timeframe 5m --watch --interval 30

    # 指定监控条件
    /opt/anaconda3/envs/trade/bin/python -m tools.monitor \
        --base HYPE --timeframe 5m \
        --alerts "ema21_reject,ema200_break,engulf,wick_long"

内置条件：
    ema21_reject  — 价格触碰/靠近 EMA21 后回落（做空信号）
    ema200_break  — 价格有效跌破 EMA200（做空信号）
    ema200_bounce — 价格回踩 EMA200 获得支撑（做多信号）
    engulf        — 出现吞没形态
    wick_long     — 出现长影线（上影≥0.5 或 下影≥0.5）
    wave_extreme  — Wave 进入超买(≥40)或超卖(≤-40)区域
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import ccxt
import pandas as pd

from .config import PROJECT_ROOT, PROXY, load_defaults


# ---------------------------------------------------------------------------
# 核心工具函数
# ---------------------------------------------------------------------------

def _build_ex(exchange_id: str, market: str) -> ccxt.Exchange:
    klass = getattr(ccxt, exchange_id)
    options = {"defaultType": "future" if market == "futures" else "spot"}
    if exchange_id == "okx" and market == "futures":
        options = {"defaultType": "swap"}
    if exchange_id == "bybit" and market == "futures":
        options = {"defaultType": "linear"}
    return klass({
        "proxies": PROXY,
        "enableRateLimit": True,
        "options": options,
        "timeout": 30000,
    })


def _build_symbol(exchange_id: str, market: str, base: str, quote: str) -> str:
    if exchange_id == "okx" and market == "futures":
        return f"{base}/{quote}:{quote}"
    return f"{base}/{quote}"


def _tf_to_minutes(tf: str) -> int:
    m = {"1m": 1, "3m": 3, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240}
    return m.get(tf, int(tf.replace("m", "").replace("h", "0")))


# ---------------------------------------------------------------------------
# 指标计算（精简版，不依赖项目内重型 indicators）
# ---------------------------------------------------------------------------

def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """给 K 线加 EMA、Wave、形态特征。"""
    df = df.copy()
    # EMA
    for p in [21, 55, 100, 200]:
        df[f"ema_{p}"] = _ema(df["close"], p)

    # Wave（简化版：RSI + StochRSI 的 Wave）
    rsi_period = 14
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(span=rsi_period, adjust=False).mean()
    avg_loss = loss.ewm(span=rsi_period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, 1e-10)
    df["rsi"] = 100 - (100 / (1 + rs))

    # Stoch RSI
    rsi_series = df["rsi"]
    stoch_period = 14
    rsi_low = rsi_series.rolling(stoch_period).min()
    rsi_high = rsi_series.rolling(stoch_period).max()
    stoch_rsi = (rsi_series - rsi_low) / (rsi_high - rsi_low + 1e-10) * 100
    df["stoch_k"] = stoch_rsi.ewm(span=3, adjust=False).mean()
    df["stoch_d"] = df["stoch_k"].ewm(span=3, adjust=False).mean()

    # Wave: 0 轴上下，±50 为极值
    df["wave"] = (df["stoch_k"] * 2 - 50).clip(-50, 50)

    # K 线形态
    df["body"] = df["close"] - df["open"]
    df["upper_wick"] = df["high"] - df[["open", "close"]].max(axis=1)
    df["lower_wick"] = df[["open", "close"]].min(axis=1) - df["low"]
    total_range = df["high"] - df["low"] + 1e-10
    df["body_ratio"] = abs(df["body"]) / total_range
    df["upper_wick_ratio"] = df["upper_wick"] / total_range
    df["lower_wick_ratio"] = df["lower_wick"] / total_range
    df["is_bull"] = df["body"] >= 0

    # 吞没
    def _engulf(i):
        if i < 1:
            return ""
        prev = df.iloc[i-1]
        cur = df.iloc[i]
        if prev["is_bull"] and not cur["is_bull"] and cur["open"] > prev["close"] and cur["close"] < prev["open"]:
            return "bear"
        if not prev["is_bull"] and cur["is_bull"] and cur["open"] < prev["close"] and cur["close"] > prev["open"]:
            return "bull"
        return ""
    df["engulf"] = [_engulf(i) for i in range(len(df))]

    # 成交量均线
    df["vol_ma"] = df["volume"].rolling(20).mean()
    df["vol_vs_ma"] = df["volume"] / df["vol_ma"].replace(0, 1)

    # EMA 排列
    def _stack(r):
        if pd.isna(r.get("ema_21")) or pd.isna(r.get("ema_200")):
            return "unknown"
        if r["ema_21"] > r["ema_55"] > r["ema_100"] > r["ema_200"]:
            return "bull_stack"
        if r["ema_21"] < r["ema_55"] < r["ema_100"] < r["ema_200"]:
            return "bear_stack"
        return "tangled"
    df["ema_stack"] = df.apply(_stack, axis=1)

    return df


def _fetch_latest(
    ex: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    n_bars: int = 500,
    base_parquet: Optional[Path] = None,
) -> pd.DataFrame:
    """拉取最近 n_bars 根 K 线并计算指标。
    
    默认拉 500 根 5m K（约 42 小时），足够 EMA200 收敛准确。
    """
    raw = ex.fetch_ohlcv(symbol, timeframe, limit=n_bars)
    df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms").dt.tz_localize("UTC").dt.tz_convert("Asia/Shanghai")
    df = _compute_indicators(df)
    return df


# ---------------------------------------------------------------------------
# 信号检测
# ---------------------------------------------------------------------------

@dataclass
class Alert:
    name: str
    direction: str       # "long" / "short" / "neutral"
    message: str
    bar_index: int       # 相对于 df 末尾的偏移（0=最新）
    price: float
    time: str


def check_alerts(df: pd.DataFrame, active: set[str]) -> list[Alert]:
    """在最新已收盘 K 线上检测条件。
    
    关键设计：始终用 df.iloc[-2] 作为检测目标（最新已收盘 K），
    避免在尚未收盘的 K 线上产生虚假信号。
    """
    alerts: list[Alert] = []
    if len(df) < 3:
        return alerts

    # ⚠️ 用 -2（最新已收盘）而非 -1（可能还在形成中）
    cur = df.iloc[-2]       # 最新已收盘 K
    prev = df.iloc[-3]      # 前一根已收盘 K
    cur_t = str(cur["datetime"])[11:16]
    prev_t = str(prev["datetime"])[11:16]

    ema21 = cur["ema_21"]
    ema200 = cur["ema_200"]
    close = cur["close"]
    high = cur["high"]
    low = cur["low"]

    # ── ema21_reject ──
    if "ema21_reject" in active:
        # 价格在 EMA21 下方 + 反弹触及/靠近 EMA21 但被压回
        if close < ema21:
            dist_pct = (ema21 - close) / close * 100
            # 当前 K 的 high 接近 EMA21（±0.3%内）且收盘远低于 high
            high_to_ema = (ema21 - high) / high * 100
            if high_to_ema < 0.3 and cur["upper_wick_ratio"] > 0.3:
                alerts.append(Alert(
                    name="ema21_reject",
                    direction="short",
                    message=f"⚠️ EMA21 拒绝 @{cur_t}: H={high:.4f}→C={close:.4f}, "
                            f"EMA21={ema21:.4f}, 上影={cur['upper_wick_ratio']:.2f}",
                    bar_index=0, price=close, time=cur_t,
                ))
            elif dist_pct < 0.5:  # 距 EMA21 不到 0.5%，密切关注
                alerts.append(Alert(
                    name="ema21_near",
                    direction="neutral",
                    message=f"👀 逼近 EMA21 @{cur_t}: C={close:.4f}, EMA21={ema21:.4f} (差{dist_pct:.2f}%)",
                    bar_index=0, price=close, time=cur_t,
                ))

    # ── ema200_break ──
    if "ema200_break" in active:
        if close < ema200:
            # 需要上一 K 还在上方（有效跌破）
            if prev["close"] >= ema200:
                alerts.append(Alert(
                    name="ema200_break",
                    direction="short",
                    message=f"🔴 EMA200 跌破 @{cur_t}: C={close:.4f}, EMA200={ema200:.4f}",
                    bar_index=0, price=close, time=cur_t,
                ))
            elif close < ema200:
                alerts.append(Alert(
                    name="ema200_below",
                    direction="short",
                    message=f"📉 持续低于 EMA200 @{cur_t}: C={close:.4f}, EMA200={ema200:.4f}",
                    bar_index=0, price=close, time=cur_t,
                ))

    # ── ema200_bounce ──
    if "ema200_bounce" in active:
        dist_pct = (close - ema200) / ema200 * 100
        if 0 <= dist_pct < 0.5 and cur["lower_wick_ratio"] > 0.3 and cur["is_bull"]:
            alerts.append(Alert(
                name="ema200_bounce",
                direction="long",
                message=f"🟢 EMA200 支撑 @{cur_t}: C={close:.4f}, EMA200={ema200:.4f}, "
                        f"下影={cur['lower_wick_ratio']:.2f}",
                bar_index=0, price=close, time=cur_t,
            ))
        elif dist_pct < 1.0 and low < ema200 * 1.005:
            alerts.append(Alert(
                name="ema200_near",
                direction="neutral",
                message=f"👀 接近 EMA200 @{cur_t}: L={low:.4f}, EMA200={ema200:.4f}",
                bar_index=0, price=close, time=cur_t,
            ))

    # ── engulf ──
    if "engulf" in active:
        eng = cur["engulf"]
        if eng == "bear":
            alerts.append(Alert(
                name="engulf_bear",
                direction="short",
                message=f"🐻 Bear Engulf @{cur_t}: O={cur['open']:.4f}→C={close:.4f}",
                bar_index=0, price=close, time=cur_t,
            ))
        elif eng == "bull":
            alerts.append(Alert(
                name="engulf_bull",
                direction="long",
                message=f"🐂 Bull Engulf @{cur_t}: O={cur['open']:.4f}→C={close:.4f}",
                bar_index=0, price=close, time=cur_t,
            ))

    # ── wick_long ──
    if "wick_long" in active:
        if cur["upper_wick_ratio"] >= 0.5:
            alerts.append(Alert(
                name="wick_upper",
                direction="short",
                message=f"📌 长上影 @{cur_t}: H={high:.4f} C={close:.4f} ratio={cur['upper_wick_ratio']:.2f}",
                bar_index=0, price=close, time=cur_t,
            ))
        if cur["lower_wick_ratio"] >= 0.5:
            alerts.append(Alert(
                name="wick_lower",
                direction="long",
                message=f"📌 长下影 @{cur_t}: L={low:.4f} C={close:.4f} ratio={cur['lower_wick_ratio']:.2f}",
                bar_index=0, price=close, time=cur_t,
            ))

    # ── wave_extreme ──
    if "wave_extreme" in active:
        w = cur["wave"]
        if w >= 40:
            alerts.append(Alert(
                name="wave_overbought",
                direction="short",
                message=f"🔥 Wave 超买 @{cur_t}: Wave={w:.1f}",
                bar_index=0, price=close, time=cur_t,
            ))
        elif w <= -40:
            alerts.append(Alert(
                name="wave_oversold",
                direction="long",
                message=f"❄️ Wave 超卖 @{cur_t}: Wave={w:.1f}",
                bar_index=0, price=close, time=cur_t,
            ))

    return alerts


# ---------------------------------------------------------------------------
# 状态面板
# ---------------------------------------------------------------------------

def print_status(df: pd.DataFrame, watch_targets: Optional[dict] = None):
    """打印当前状态面板。展示最新已收盘 K + 正在形成的 K（如有）。"""
    live = df.iloc[-1]         # 可能还在形成中
    closed = df.iloc[-2]        # 最新已收盘
    ema21 = closed["ema_21"]
    ema200 = closed["ema_200"]
    close = closed["close"]
    wave = closed["wave"]
    stack = closed["ema_stack"]
    vol_ratio = closed["vol_vs_ma"]
    eng = closed["engulf"]
    closed_t = str(closed["datetime"])[11:16]
    live_t = str(live["datetime"])[11:16]

    print()
    print("━" * 55)
    if closed_t != live_t:
        print(f"  HYPE/USDT 5m  | 已收盘: {closed_t} C={close:.4f}  |  实时: {live_t} C={live['close']:.4f}")
    else:
        print(f"  HYPE/USDT 5m  @ {closed_t}  | C={close:.4f}")
    print("━" * 55)

    # EMA 关系
    d21 = (close - ema21) / close * 100
    d200 = (close - ema200) / close * 100
    ema21_s = "🔴低于" if d21 < 0 else "🟢高于"
    ema200_s = "🔴低于" if d200 < 0 else "🟢高于"
    print(f"  EMA21:  {ema21:.4f}  {ema21_s} {abs(d21):.2f}%")
    print(f"  EMA200: {ema200:.4f}  {ema200_s} {abs(d200):.2f}%")
    print(f"  Wave:   {wave:.1f}")
    print(f"  Stack:  {stack}")
    print(f"  量比:   {vol_ratio:.2f}x")
    if eng:
        print(f"  形态:   {eng} engulf")

    # 监控目标
    if watch_targets:
        print()
        print("  ── 监控目标 ──")
        for label, price in watch_targets.items():
            dist = (price - close) / close * 100
            arrow = "▲" if dist > 0 else "▼"
            print(f"  {label}: {price:.4f}  {arrow} {abs(dist):.2f}%")

    print("━" * 55)


# ---------------------------------------------------------------------------
# 主循环
# ---------------------------------------------------------------------------

ALL_ALERTS = {"ema21_reject", "ema200_break", "ema200_bounce", "engulf", "wick_long", "wave_extreme"}


def _main():
    p = argparse.ArgumentParser(description="实时价格监控 + 条件信号检测")
    p.add_argument("--base", default="HYPE")
    p.add_argument("--quote", default="USDT")
    p.add_argument("--timeframe", default="5m")
    p.add_argument("--exchange", default="binance")
    p.add_argument("--market", default="futures")
    p.add_argument("--watch", action="store_true", help="持续监控模式")
    p.add_argument("--interval", type=int, default=30, help="检查间隔秒数（默认 30s）")
    p.add_argument("--alerts", default="ema21_reject,ema200_break,ema200_bounce,engulf,wick_long",
                   help=f"要检测的条件，逗号分隔。可选: {','.join(sorted(ALL_ALERTS))}")
    args = p.parse_args()

    active = set(a.strip() for a in args.alerts.split(",") if a.strip() in ALL_ALERTS)
    invalid = set(a.strip() for a in args.alerts.split(",")) - ALL_ALERTS
    if invalid:
        print(f"[warn] 未知条件忽略: {invalid}")

    ex = _build_ex(args.exchange, args.market)
    symbol = _build_symbol(args.exchange, args.market, args.base, args.quote)

    # 监控目标位
    df_init = _fetch_latest(ex, symbol, args.timeframe)
    cur = df_init.iloc[-2]  # 最新已收盘
    watch_targets = {
        "EMA21":  cur["ema_21"],
        "EMA200": cur["ema_200"],
        "前低B92": 44.589,
    }

    print(f"\n🛰️ HYPE 5m 监控启动  {datetime.now().strftime('%H:%M:%S')}")
    print(f"   检测条件: {', '.join(sorted(active))}")
    print(f"   {'持续模式' if args.watch else '单次检查'}")

    def _check():
        df = _fetch_latest(ex, symbol, args.timeframe)
        # 更新目标位
        c = df.iloc[-1]
        watch_targets["EMA21"] = c["ema_21"]
        watch_targets["EMA200"] = c["ema_200"]
        print_status(df, watch_targets)

        alerts = check_alerts(df, active)
        if alerts:
            print(f"\n  ═══ {'='*45}")
            for a in alerts:
                icon = {"long": "🟢 LONG", "short": "🔴 SHORT", "neutral": "⚪"}[a.direction]
                print(f"  {icon} [{a.name}] {a.message}")
            print(f"  ═══ {'='*45}")
        else:
            print(f"  ✓ 无信号触发")
        return any(a.direction != "neutral" for a in alerts)

    if args.watch:
        print(f"\n  (每 {args.interval}s 检查，Ctrl+C 退出)")
        triggered = False
        try:
            while True:
                triggered = _check()
                if triggered:
                    print(f"\n  🔊 信号触发！持续监控中...")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print(f"\n\n👋 监控结束  {datetime.now().strftime('%H:%M:%S')}")
    else:
        _check()


if __name__ == "__main__":
    _main()
