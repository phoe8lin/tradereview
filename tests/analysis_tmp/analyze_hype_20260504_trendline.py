from pathlib import Path
import json
import sys

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from tools.validators import load_day, get_bar, detect_features, regime_summary, regime_segments


def fmt_bar(bar):
    fields = [
        "id", "datetime", "open", "high", "low", "close", "volume", "vol_vs_ma",
        "ema_21", "ema_55", "ema_100", "ema_200", "ema_stack", "wave",
        "body_ratio", "upper_wick_ratio", "lower_wick_ratio", "engulf", "delta", "cvd", "buy_ratio",
    ]
    out = {}
    for f in fields:
        v = bar.get(f)
        if isinstance(v, pd.Timestamp):
            out[f] = v.strftime("%Y-%m-%d %H:%M")
        elif isinstance(v, float):
            out[f] = round(v, 6)
        else:
            out[f] = v
    out["features"] = detect_features(bar).to_dict()["patterns"]
    return out


def seg_stats(df, start, end):
    sub = df[(df["datetime"].dt.strftime("%H:%M") >= start) & (df["datetime"].dt.strftime("%H:%M") <= end)].copy()
    if sub.empty:
        return {"start": start, "end": end, "empty": True}
    return {
        "start": start, "end": end, "bars": len(sub),
        "delta_sum": round(float(sub["delta"].sum()), 1),
        "cvd_start": round(float(sub["cvd"].iloc[0]), 1),
        "cvd_end": round(float(sub["cvd"].iloc[-1]), 1),
        "cvd_change": round(float(sub["cvd"].iloc[-1] - sub["cvd"].iloc[0]), 1),
        "buy_ratio_mean": round(float(sub["buy_ratio"].mean()), 3),
        "delta_pos": int((sub["delta"] > 0).sum()),
        "delta_neg": int((sub["delta"] < 0).sum()),
    }


def main():
    df = load_day("2026-05-04", "HYPE", "5m")

    # 三个高点
    b156 = get_bar(df, "13:00")
    b190 = get_bar(df, "15:50")
    b214 = get_bar(df, "17:50")

    # 趋势线：连接 15:50 和 17:50 高点
    t190_m = 15 * 60 + 50
    t214_m = 17 * 60 + 50
    h190 = float(b190["high"])
    h214 = float(b214["high"])
    slope = (h214 - h190) / (t214_m - t190_m)

    t156_m = 13 * 60
    tl_at_1300 = h190 + slope * (t156_m - t190_m)
    h156 = float(b156["high"])

    # 18:00 大跌
    b216 = get_bar(df, "18:00")
    b217 = get_bar(df, "18:05")

    # 12:45-18:05 区间所有高点排名
    mid = df[(df["datetime"].dt.strftime("%H:%M") >= "12:45") & (df["datetime"].dt.strftime("%H:%M") <= "17:55")]
    top5 = mid.nlargest(5, "high")[["id", "datetime", "high", "wave", "delta", "vol_vs_ma"]].copy()
    top5["time"] = top5["datetime"].dt.strftime("%H:%M")
    top5_list = []
    for _, r in top5.iterrows():
        top5_list.append({
            "id": r["id"], "time": r["time"],
            "high": round(float(r["high"]), 4),
            "wave": round(float(r["wave"]), 2),
            "delta": round(float(r["delta"]), 1),
            "vol_vs_ma": round(float(r["vol_vs_ma"]), 3),
        })

    # 17:30-18:10 序列
    seq = df[(df["datetime"].dt.strftime("%H:%M") >= "17:30") & (df["datetime"].dt.strftime("%H:%M") <= "18:10")]
    seq_list = []
    for _, r in seq.iterrows():
        seq_list.append({
            "id": r["id"], "time": r["datetime"].strftime("%H:%M"),
            "open": round(float(r["open"]), 3), "high": round(float(r["high"]), 3),
            "low": round(float(r["low"]), 3), "close": round(float(r["close"]), 3),
            "wave": round(float(r["wave"]), 2),
            "delta": round(float(r["delta"]), 1),
            "cvd": round(float(r["cvd"]), 1),
            "vol_vs_ma": round(float(r["vol_vs_ma"]), 3),
            "engulf": r["engulf"],
        })

    out = {
        "regime_summary": regime_summary(df),
        "regime_segments": regime_segments(df),
        "trendline": {
            "point1_B190_1550": {"high": h190, "wave": round(float(b190["wave"]), 2)},
            "point2_B214_1750": {"high": h214, "wave": round(float(b214["wave"]), 2)},
            "slope_per_min": round(slope, 6),
            "trendline_at_1300": round(tl_at_1300, 4),
            "B156_1300_high": h156,
            "B156_above_trendline": round(h156 - tl_at_1300, 4),
        },
        "three_highs": {
            "B156_1300": fmt_bar(b156),
            "B190_1550": fmt_bar(b190),
            "B214_1750": fmt_bar(b214),
        },
        "drop_bar_B216_1800": fmt_bar(b216),
        "drop_bar_B217_1805": fmt_bar(b217),
        "top5_highs_1245_1755": top5_list,
        "segments": {
            "12:45-13:15": seg_stats(df, "12:45", "13:15"),
            "15:35-16:05": seg_stats(df, "15:35", "16:05"),
            "17:30-18:10": seg_stats(df, "17:30", "18:10"),
            "17:50-18:10": seg_stats(df, "17:50", "18:10"),
            "12:00-18:00": seg_stats(df, "12:00", "18:00"),
        },
        "sequence_1730_1810": seq_list,
    }

    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
