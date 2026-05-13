from pathlib import Path
import json
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from tools.validators import load_day, get_bar, detect_features, regime_summary


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


def main():
    df = load_day("2026-05-04", "HYPE", "5m")

    b226 = get_bar(df, "18:50")  # 前高
    b243 = get_bar(df, "20:15")  # 被吞没的 K
    b244 = get_bar(df, "20:20")  # 入场 K

    # 入场参数
    entry = float(b244["close"])
    stop = float(b244["high"]) + 0.002
    risk = stop - entry

    # 寻找 RR > 1.3 的目标
    target_rr13 = entry - risk * 1.3

    # 前低参考
    pre_lows = df[(df["datetime"].dt.strftime("%H:%M") >= "18:10") & (df["datetime"].dt.strftime("%H:%M") <= "20:15")]
    low_1810 = float(get_bar(df, "18:10")["low"])  # 41.074
    low_1925 = float(get_bar(df, "19:25")["low"])  # 41.278
    low_1930 = float(get_bar(df, "19:30")["low"])  # 41.235

    # 后验
    after = df.iloc[list(df["datetime"].dt.strftime("%H:%M")).index("20:20") + 1:]
    post_high = float(after["high"].max())
    post_low = float(after["low"].min())
    mae = max(0, post_high - entry)
    mfe = entry - post_low

    # EMA200 趋势
    ema200_change = round(float(b244["ema_200"] - b226["ema_200"]), 4)

    out = {
        "regime": regime_summary(df),
        "key_bars": {
            "B226_1850_prior_high": fmt_bar(b226),
            "B243_2015_engulfed": fmt_bar(b243),
            "B244_2020_entry": fmt_bar(b244),
        },
        "trade_plan": {
            "entry": round(entry, 3),
            "stop": round(stop, 3),
            "risk": round(risk, 3),
            "target_for_rr_1.3": round(target_rr13, 3),
            "candidate_targets": {
                "B233_1925_low_41.278": {
                    "target": 41.278,
                    "reward": round(entry - 41.278, 3),
                    "rr": round((entry - 41.278) / risk, 2),
                },
                "B234_1930_low_41.235": {
                    "target": 41.235,
                    "reward": round(entry - 41.235, 3),
                    "rr": round((entry - 41.235) / risk, 2),
                },
                "B218_1810_low_41.074": {
                    "target": 41.074,
                    "reward": round(entry - 41.074, 3),
                    "rr": round((entry - 41.074) / risk, 2),
                },
            },
        },
        "ema200_trend": {
            "B226_1850": round(float(b226["ema_200"]), 3),
            "B244_2020": round(float(b244["ema_200"]), 3),
            "change": ema200_change,
        },
        "post_entry": {
            "highest_after": round(post_high, 3),
            "lowest_after": round(post_low, 3),
            "mae": round(mae, 3),
            "mfe": round(mfe, 3),
            "stop_hit": post_high >= stop,
        },
    }

    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
