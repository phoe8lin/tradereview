"""日级别多笔 review 生成器：扫描一日行情 + 订单流，生成数据文件和 review.md 骨架。

与 review_builder.py 的区别：
- review_builder.py —— 单笔交易（有锚 K）：数据 + Plotly 图表 + trade.yaml
- day_review_builder.py —— 全日扫描（无锚 K）：当日完整 K 线 + 订单流 + review 骨架

典型用法：

    # 仅主周期（15m 全日）
    /opt/anaconda3/envs/trade/bin/python -m tools.day_review_builder \
        --date 2026-04-21 --base HYPE --timeframe 15m

    # 主周期 + 辅周期窗口（比如某段时间细看 5m）
    /opt/anaconda3/envs/trade/bin/python -m tools.day_review_builder \
        --date 2026-04-21 --base HYPE --timeframe 15m \
        --supp-tf 5m --supp-window "20:00-23:45"

产物：
    reviews/<date>/data/day_<BASE>_<TF>.{parquet,txt}
    reviews/<date>/data/supp_<BASE>_<SUPP_TF>.{parquet,txt}   (可选)
    reviews/<date>/review.md                    (若不存在则写骨架)
    reviews/<date>/review.skeleton.md           (每次刷新)

幂等性：数据文件每次覆盖；review.md 首次写入后不覆盖；review.skeleton.md 每次刷新。
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from .config import PROJECT_ROOT, ensure_dirs, load_defaults
from .data_fetcher import FetchSpec, fetch_around_anchor, TZ_CN, _TF_MS
from .indicators import add_ema, add_wave_filter, classify_ema_stack
from .kline_features import add_kline_features
from .orderflow_fetcher import fetch_trades_window, compute_orderflow


# ---------------------------------------------------------------------------
# 数据拉取与指标合并
# ---------------------------------------------------------------------------

def _fetch_range_with_indicators(
    base: str,
    quote: str,
    timeframe: str,
    start_cn: pd.Timestamp,
    end_cn: pd.Timestamp,
    exchange: str,
    market: str,
    cfg: dict,
) -> pd.DataFrame:
    """以 [start_cn, end_cn] 窗口为中心拉 K 线并挂上全部指标。"""
    mid = start_cn + (end_cn - start_cn) / 2
    anchor_cn = mid.strftime("%Y-%m-%d %H:%M")
    bars_after = int((end_cn - pd.Timestamp(anchor_cn).tz_localize(TZ_CN)) / pd.Timedelta(_TF_MS[timeframe], unit="ms")) + 2

    spec = FetchSpec(
        exchange=exchange, market=market,
        base=base, quote=quote,
        timeframe=timeframe,
        anchor_cn=anchor_cn,
        bars_before=cfg["data"]["warmup_bars"],
        bars_after=bars_after,
    )
    df = fetch_around_anchor(spec)

    add_ema(df, cfg["ema"]["periods"])
    wf = cfg["wave_filter"]
    add_wave_filter(
        df,
        rsi_period=wf["rsi_period"], stoch_period=wf["stoch_period"],
        smooth_k=wf["smooth_k"], smooth_d=wf["smooth_d"],
        ma_type=wf["ma_type"], source=wf["source"],
    )
    add_kline_features(df)
    df["ema_stack"] = df.apply(
        lambda r: classify_ema_stack(r, tuple(cfg["ema"]["periods"])), axis=1
    )
    return df


def _slice_and_label(df: pd.DataFrame, start_cn: pd.Timestamp, end_cn: pd.Timestamp, prefix: str) -> pd.DataFrame:
    """截到 [start_cn, end_cn) 窗口，并给每根 K 线一个 <prefix>NN 编号。"""
    s_ms = int(start_cn.tz_convert("UTC").timestamp() * 1000)
    e_ms = int(end_cn.tz_convert("UTC").timestamp() * 1000)
    win = df[(df["timestamp"] >= s_ms) & (df["timestamp"] < e_ms)].reset_index(drop=True).copy()
    win["id"] = [f"{prefix}{i:02d}" for i in range(len(win))]
    return win


def _attach_orderflow(
    win: pd.DataFrame, base: str, quote: str, timeframe: str,
    exchange: str, market: str, cfg: dict,
) -> pd.DataFrame:
    """给窗口 K 线挂上订单流指标（当次新拉 aggTrades，不走缓存）。"""
    if win.empty:
        return win
    tf_ms = _TF_MS[timeframe]
    start_ms = int(win["timestamp"].iloc[0])
    end_ms = int(win["timestamp"].iloc[-1]) + tf_ms

    trades = fetch_trades_window(
        exchange, market, base, quote,
        start_ms, end_ms, limit=cfg["orderflow"]["trades_limit"],
    )
    of = compute_orderflow(
        win.rename(columns={"id": "kline_id"})[["kline_id", "datetime", "timestamp", "close"]],
        trades, tf_ms,
    )
    return win.merge(
        of[["timestamp", "buy_vol", "sell_vol", "total_trades_vol", "delta", "cvd", "buy_ratio"]],
        on="timestamp", how="left",
    )


# ---------------------------------------------------------------------------
# 落盘
# ---------------------------------------------------------------------------

_DISPLAY_COLS = [
    "id", "datetime", "open", "high", "low", "close",
    "volume", "vol_vs_ma",
    "ema_21", "ema_55", "ema_100", "ema_200", "ema_stack",
    "wave", "body_ratio", "upper_wick_ratio", "lower_wick_ratio", "engulf",
    "delta", "cvd", "buy_ratio",
]


def _save(df: pd.DataFrame, parquet_path: Path, txt_path: Path) -> None:
    """落 parquet + 可读 txt（parquet 内 datetime 去 tz）。"""
    to_save = df.copy()
    to_save["datetime"] = to_save["datetime"].dt.tz_localize(None)
    to_save.to_parquet(parquet_path, index=False)

    display = df.copy()
    display["time"] = display["datetime"].dt.strftime("%m-%d %H:%M")
    cols = ["id", "time"] + [c for c in _DISPLAY_COLS if c not in ("id", "datetime")]
    cols = [c for c in cols if c in display.columns]
    with open(txt_path, "w", encoding="utf-8") as f:
        display[cols].to_string(f, index=False, float_format=lambda x: f"{x:.3f}")


# ---------------------------------------------------------------------------
# 骨架生成
# ---------------------------------------------------------------------------

def _summarize_day(day: pd.DataFrame, cfg: dict) -> dict:
    """提取骨架需要的统计量。"""
    if day.empty:
        return {}

    long_wick_th = cfg["features"]["long_wick_ratio"]
    vol_spike_th = cfg["features"]["vol_spike_multiple"]

    # EMA100 触碰：收盘站上 + high 触及
    ema100_above_close = day[day["close"] > day["ema_100"]]["id"].tolist()
    ema100_high_tag = day[(day["high"] >= day["ema_100"]) & (day["close"] < day["ema_100"])]["id"].tolist()

    return {
        "n_bars": len(day),
        "open": day["open"].iloc[0],
        "high": day["high"].max(),
        "high_id": day.loc[day["high"].idxmax(), "id"],
        "low": day["low"].min(),
        "low_id": day.loc[day["low"].idxmin(), "id"],
        "close": day["close"].iloc[-1],
        "range_pct": (day["high"].max() - day["low"].min()) / day["open"].iloc[0] * 100,
        "cvd_final": day["cvd"].iloc[-1] if "cvd" in day else None,
        "delta_sum": day["delta"].sum() if "delta" in day else None,
        "buy_ratio_mean": day["buy_ratio"].mean() if "buy_ratio" in day else None,
        "ema_stack_counts": day["ema_stack"].value_counts().to_dict(),
        "long_upper_ids": day[day["upper_wick_ratio"] >= long_wick_th]["id"].tolist(),
        "long_lower_ids": day[day["lower_wick_ratio"] >= long_wick_th]["id"].tolist(),
        "vol_spike_ids": day[day["vol_vs_ma"] >= vol_spike_th]["id"].tolist(),
        "engulf_ids": day[day["engulf"] != ""][["id", "engulf"]].values.tolist(),
        "ema100_close_above_ids": ema100_above_close,
        "ema100_high_tag_ids": ema100_high_tag,
        "wave_max": day["wave"].max(),
        "wave_max_id": day.loc[day["wave"].idxmax(), "id"] if not day["wave"].isna().all() else None,
        "wave_min": day["wave"].min(),
        "wave_min_id": day.loc[day["wave"].idxmin(), "id"] if not day["wave"].isna().all() else None,
    }


def _render_skeleton(
    date: str, base: str, quote: str, timeframe: str,
    exchange: str, market: str,
    day_stats: dict,
    supp_tf: Optional[str] = None,
    supp_window: Optional[str] = None,
    supp_stats: Optional[dict] = None,
    day_data_rel: str = "", supp_data_rel: str = "",
) -> str:
    """生成 review.md 骨架文本。"""
    lines = []
    push = lines.append

    push(f"# {base}/{quote} {'永续' if market=='futures' else '现货'} · {date} 日复盘")
    push("")
    push(f"**主周期**: {timeframe}  " + (f"**辅周期**: {supp_tf}（窗口 {supp_window}）  " if supp_tf else "") +
         f"**时区**: UTC+8  **交易所**: {exchange.upper()} {'USDT-M Perp' if market=='futures' else 'Spot'}")
    push("")
    push("---")
    push("")

    # 一、日概览
    push("## 一、日概览")
    push("")
    push("| 项 | 值 |")
    push("|---|---|")
    push(f"| 开 | {day_stats['open']:.4f} |")
    push(f"| 日内最高 ({day_stats['high_id']}) | {day_stats['high']:.4f} |")
    push(f"| 日内最低 ({day_stats['low_id']}) | {day_stats['low']:.4f} |")
    push(f"| 收 | {day_stats['close']:.4f} |")
    push(f"| 日振幅 | {day_stats['range_pct']:.2f}% |")
    bear = day_stats["ema_stack_counts"].get("bear_stack", 0)
    bull = day_stats["ema_stack_counts"].get("bull_stack", 0)
    tangled = day_stats["ema_stack_counts"].get("tangled", 0)
    push(f"| EMA 排列 | bear {bear} / bull {bull} / tangled {tangled}（共 {day_stats['n_bars']} 根） |")
    if day_stats.get("cvd_final") is not None:
        push(f"| 全日 delta 合计 | {day_stats['delta_sum']:+,.0f} |")
        push(f"| 收盘 CVD | {day_stats['cvd_final']:+,.0f} |")
        push(f"| buy_ratio 均值 | {day_stats['buy_ratio_mean']:.3f} |")
    push("")
    push("**结构定性**：_（人工填写：趋势日/震荡日/反转日；主方向；结构参考位）_")
    push("")

    # 二、关键结构（自动识别候选，人工筛选）
    push("## 二、日内关键位与信号候选（自动识别）")
    push("")
    push(f"- **收盘站上 EMA100**（{len(day_stats['ema100_close_above_ids'])} 根）: `{day_stats['ema100_close_above_ids']}`")
    push(f"- **仅 high 触 EMA100 但收盘不站上**（{len(day_stats['ema100_high_tag_ids'])} 根）: `{day_stats['ema100_high_tag_ids']}`")
    push(f"- **长上影线**（upper_wick ≥ 0.5）: `{day_stats['long_upper_ids']}`")
    push(f"- **长下影线**（lower_wick ≥ 0.5）: `{day_stats['long_lower_ids']}`")
    push(f"- **放量 K 线**（vol_vs_ma ≥ 2.0）: `{day_stats['vol_spike_ids']}`")
    push(f"- **吞没形态**: `{day_stats['engulf_ids']}`")
    if day_stats.get("wave_max_id"):
        push(f"- **Wave 峰值 (max)**: {day_stats['wave_max']:.1f} @ {day_stats['wave_max_id']}")
        push(f"- **Wave 谷值 (min)**: {day_stats['wave_min']:.1f} @ {day_stats['wave_min_id']}")
    push("")
    push("**关键位映射表**（人工填写）:")
    push("")
    push("| 次 | K 线 | 时间 | 价格位 | 形态 | 意义 |")
    push("|---|---|---|---|---|---|")
    push("| 1 | - | - | - | - | - |")
    push("")

    # 三、做空/做多点位表
    push("## 三、日内交易点位全景表")
    push("")
    push("按时间顺序列出所有识别到的做多/做空机会，含**信号强度**与**可交易性**双维度评价。")
    push("")
    push("| # | K 线 | 时间 | 方向 | 入场 | 止损 | 止盈 | R:R | 信号强度 | 可交易 | 决策 | 结果 |")
    push("|---|---|---|---|---|---|---|---|---|---|---|---|")
    push("| 1 | - | - | - | - | - | - | - | - | - | - | - |")
    push("")

    # 四、订单流洞察
    push("## 四、订单流与形态互证")
    push("")
    push("_（人工填写：价格-CVD 背离点 / 吸收点 / 推动-衰竭模板案例）_")
    push("")

    # 五、方法论沉淀
    push("## 五、方法论沉淀")
    push("")
    push("### 5.1 当日最重要的认知")
    push("")
    push("_（人工填写）_")
    push("")

    # 五之外：辅周期
    if supp_tf and supp_stats:
        push(f"## 六、辅周期补充：{supp_tf}（窗口 {supp_window}）")
        push("")
        push(f"- 窗口内 K 线数: {supp_stats['n_bars']}")
        push(f"- 窗口最高 / 最低: {supp_stats['high']:.4f} ({supp_stats['high_id']}) / {supp_stats['low']:.4f} ({supp_stats['low_id']})")
        push(f"- 长上影: `{supp_stats['long_upper_ids']}`")
        push(f"- 长下影: `{supp_stats['long_lower_ids']}`")
        push(f"- 放量: `{supp_stats['vol_spike_ids']}`")
        push(f"- 吞没: `{supp_stats['engulf_ids']}`")
        push("")
        push("_（人工填写：相对主周期的哪段用此窗口精化？有无 15m 看不到的 trigger？）_")
        push("")

    # 心得标签
    push("## 七、心得标签")
    push("")
    push("`#tag1` `#tag2`  _（人工填写，方便跨复盘检索）_")
    push("")

    # 产物
    push("## 八、产物清单")
    push("")
    push("| 文件 | 用途 |")
    push("|---|---|")
    push(f"| `{day_data_rel}` | 主周期 K 线 + 指标 + 订单流 |")
    if supp_tf:
        push(f"| `{supp_data_rel}` | 辅周期窗口 K 线 + 指标 + 订单流 |")
    push("")

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def build_day_review(
    date: str, base: str, quote: str, timeframe: str,
    exchange: str = "binance", market: str = "futures",
    supp_tf: Optional[str] = None, supp_window: Optional[str] = None,
    fetch_orderflow: bool = True,
) -> dict:
    """生成日复盘数据 + review.md 骨架。返回产出路径字典。"""
    ensure_dirs()
    cfg = load_defaults()

    # 时间范围
    day_start = pd.Timestamp(f"{date} 00:00").tz_localize(TZ_CN)
    day_end_req = day_start + timedelta(days=1)
    now_cn = pd.Timestamp.now(tz=TZ_CN)
    day_end = min(day_end_req, now_cn.ceil(f"{_TF_MS[timeframe]//60000}min"))

    # 主周期
    df_main = _fetch_range_with_indicators(
        base, quote, timeframe, day_start, day_end,
        exchange, market, cfg,
    )
    day = _slice_and_label(df_main, day_start, day_end, prefix="B")
    print(f"[ok] {timeframe} 主周期 {len(day)} 根")

    if fetch_orderflow and not day.empty:
        day = _attach_orderflow(day, base, quote, timeframe, exchange, market, cfg)
        print(f"[ok] 主周期订单流合并完成")

    # 辅周期（可选）
    supp = None
    if supp_tf:
        if not supp_window:
            raise ValueError("--supp-window 必填，如 '20:00-23:45'")
        s, e = [x.strip() for x in supp_window.split("-")]
        supp_start = pd.Timestamp(f"{date} {s}").tz_localize(TZ_CN)
        supp_end = pd.Timestamp(f"{date} {e}").tz_localize(TZ_CN)
        if supp_end <= supp_start:
            supp_end += timedelta(days=1)

        df_supp = _fetch_range_with_indicators(
            base, quote, supp_tf, supp_start, supp_end,
            exchange, market, cfg,
        )
        supp = _slice_and_label(df_supp, supp_start, supp_end, prefix="E")
        print(f"[ok] {supp_tf} 辅周期 {len(supp)} 根")
        if fetch_orderflow and not supp.empty:
            supp = _attach_orderflow(supp, base, quote, supp_tf, exchange, market, cfg)
            print(f"[ok] 辅周期订单流合并完成")

    # 目录和落盘
    review_root = PROJECT_ROOT / "reviews" / date
    (review_root / "data").mkdir(parents=True, exist_ok=True)
    (review_root / "raw").mkdir(parents=True, exist_ok=True)
    (review_root / "replicated").mkdir(parents=True, exist_ok=True)
    (review_root / "trades").mkdir(parents=True, exist_ok=True)

    day_pq = review_root / "data" / f"day_{base}_{timeframe}.parquet"
    day_txt = review_root / "data" / f"day_{base}_{timeframe}.txt"
    _save(day, day_pq, day_txt)
    print(f"[saved] {day_pq.relative_to(PROJECT_ROOT)}")
    print(f"[saved] {day_txt.relative_to(PROJECT_ROOT)}")

    supp_pq_rel = ""
    if supp is not None:
        supp_pq = review_root / "data" / f"supp_{base}_{supp_tf}.parquet"
        supp_txt = review_root / "data" / f"supp_{base}_{supp_tf}.txt"
        _save(supp, supp_pq, supp_txt)
        supp_pq_rel = str(supp_pq.relative_to(PROJECT_ROOT))
        print(f"[saved] {supp_pq.relative_to(PROJECT_ROOT)}")
        print(f"[saved] {supp_txt.relative_to(PROJECT_ROOT)}")

    # 骨架
    day_stats = _summarize_day(day, cfg)
    supp_stats = _summarize_day(supp, cfg) if supp is not None else None
    skeleton_md = _render_skeleton(
        date, base, quote, timeframe, exchange, market,
        day_stats,
        supp_tf=supp_tf, supp_window=supp_window, supp_stats=supp_stats,
        day_data_rel=str(day_pq.relative_to(PROJECT_ROOT)),
        supp_data_rel=supp_pq_rel,
    )

    skel_path = review_root / "review.skeleton.md"
    skel_path.write_text(skeleton_md, encoding="utf-8")
    print(f"[saved] {skel_path.relative_to(PROJECT_ROOT)}")

    review_md = review_root / "review.md"
    if not review_md.exists():
        review_md.write_text(skeleton_md, encoding="utf-8")
        print(f"[saved] {review_md.relative_to(PROJECT_ROOT)}  (首次创建)")
    else:
        print(f"[keep]  {review_md.relative_to(PROJECT_ROOT)}  已存在，未覆盖；请对照 review.skeleton.md 手动合并新增信息")

    return {
        "day_parquet": str(day_pq.relative_to(PROJECT_ROOT)),
        "supp_parquet": supp_pq_rel,
        "review_md": str(review_md.relative_to(PROJECT_ROOT)),
        "skeleton_md": str(skel_path.relative_to(PROJECT_ROOT)),
    }


def _main():
    p = argparse.ArgumentParser(description="日级别多笔 review 生成器（数据 + 骨架）")
    p.add_argument("--date", required=True, help="复盘日期 YYYY-MM-DD (UTC+8)")
    p.add_argument("--base", required=True)
    p.add_argument("--quote", default="USDT")
    p.add_argument("--timeframe", default="15m", help="主周期, 如 5m / 15m / 1h")
    p.add_argument("--exchange", default="binance")
    p.add_argument("--market", default="futures", choices=["spot", "futures"])
    p.add_argument("--supp-tf", default=None, help="辅周期周期（可选），如 5m")
    p.add_argument("--supp-window", default=None, help="辅周期窗口 HH:MM-HH:MM")
    p.add_argument("--no-orderflow", action="store_true", help="跳过订单流拉取")
    args = p.parse_args()

    result = build_day_review(
        date=args.date, base=args.base, quote=args.quote, timeframe=args.timeframe,
        exchange=args.exchange, market=args.market,
        supp_tf=args.supp_tf, supp_window=args.supp_window,
        fetch_orderflow=not args.no_orderflow,
    )
    print("\n[DONE]", result)


if __name__ == "__main__":
    _main()
