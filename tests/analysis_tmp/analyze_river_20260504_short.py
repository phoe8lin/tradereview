from pathlib import Path
import json
import sys

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from tools.validators import load_day, get_bar, parse_ref, detect_features, regime_summary, regime_segments


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


def segment_stats(df, start, end):
    sub = df[(df["datetime"].dt.strftime("%H:%M") >= start) & (df["datetime"].dt.strftime("%H:%M") <= end)].copy()
    if sub.empty:
        return {"start": start, "end": end, "empty": True}
    low_i = sub["low"].idxmin()
    high_i = sub["high"].idxmax()
    return {
        "start": start,
        "end": end,
        "bars": len(sub),
        "low_id": sub.loc[low_i, "id"],
        "low_time": sub.loc[low_i, "datetime"].strftime("%H:%M"),
        "low": round(float(sub.loc[low_i, "low"]), 6),
        "high_id": sub.loc[high_i, "id"],
        "high_time": sub.loc[high_i, "datetime"].strftime("%H:%M"),
        "high": round(float(sub.loc[high_i, "high"]), 6),
        "delta_sum": round(float(sub["delta"].sum()), 3),
        "cvd_start": round(float(sub["cvd"].iloc[0]), 3),
        "cvd_end": round(float(sub["cvd"].iloc[-1]), 3),
        "cvd_change": round(float(sub["cvd"].iloc[-1] - sub["cvd"].iloc[0]), 3),
        "buy_ratio_mean": round(float(sub["buy_ratio"].mean()), 3),
        "vol_vs_ma_mean": round(float(sub["vol_vs_ma"].mean()), 3),
        "delta_pos_bars": int((sub["delta"] > 0).sum()),
        "delta_neg_bars": int((sub["delta"] < 0).sum()),
    }


def main():
    df = load_day("2026-05-04", "RIVER", "5m")
    anchor = get_bar(df, "14:05")
    entry = float(anchor["close"])
    stop_candidates = {
        "14:05_high_plus_tick": float(anchor["high"]) + 0.001,
        "ema200_plus_tick": float(anchor["ema_200"]) + 0.001,
        "07_10_swing_high_plus_tick": float(df[df["datetime"].dt.strftime("%H:%M").between("06:45", "10:45")]["high"].max()) + 0.001,
    }
    targets = {
        "09_12_low": float(df[df["datetime"].dt.strftime("%H:%M").between("09:00", "12:00")]["low"].min()),
        "12_20_low": float(get_bar(df, "12:20")["low"]),
        "08_55_09_20_low_zone": float(df[df["datetime"].dt.strftime("%H:%M").between("08:55", "09:20")]["low"].min()),
    }
    rr = {}
    for s_name, stop in stop_candidates.items():
        risk = stop - entry
        rr[s_name] = {}
        for t_name, take in targets.items():
            reward = entry - take
            rr[s_name][t_name] = None if risk <= 0 else round(reward / risk, 3)

    after = df.iloc[parse_ref(df, "14:05") + 1:].copy()
    post_low_i = after["low"].idxmin() if not after.empty else None
    post_high_i = after["high"].idxmax() if not after.empty else None

    out = {
        "regime_summary": regime_summary(df),
        "regime_segments": regime_segments(df),
        "key_bars": {
            "07:00": fmt_bar(get_bar(df, "07:00")),
            "07:05": fmt_bar(get_bar(df, "07:05")),
            "10:00": fmt_bar(get_bar(df, "10:00")),
            "10:05": fmt_bar(get_bar(df, "10:05")),
            "14:00": fmt_bar(get_bar(df, "14:00")),
            "14:05_anchor": fmt_bar(anchor),
            "14:10": fmt_bar(get_bar(df, "14:10")),
            "14:20": fmt_bar(get_bar(df, "14:20")),
        },
        "segments": {
            "06:45-07:30": segment_stats(df, "06:45", "07:30"),
            "09:00-12:00": segment_stats(df, "09:00", "12:00"),
            "09:00-09:30": segment_stats(df, "09:00", "09:30"),
            "10:00-10:45": segment_stats(df, "10:00", "10:45"),
            "13:45-14:25": segment_stats(df, "13:45", "14:25"),
            "14:05-14:35": segment_stats(df, "14:05", "14:35"),
        },
        "ema200_slope": {
            "07:00_to_10:00": round(float(get_bar(df, "10:00")["ema_200"] - get_bar(df, "07:00")["ema_200"]), 6),
            "10:00_to_14:05": round(float(anchor["ema_200"] - get_bar(df, "10:00")["ema_200"]), 6),
            "13:00_to_14:05": round(float(anchor["ema_200"] - get_bar(df, "13:00")["ema_200"]), 6),
        },
        "trade_plan": {
            "entry_at_14:05_close": round(entry, 6),
            "stop_candidates": {k: round(v, 6) for k, v in stop_candidates.items()},
            "targets": {k: round(v, 6) for k, v in targets.items()},
            "rr": rr,
        },
        "post_14:05": None if after.empty else {
            "lowest_after_id": df.loc[post_low_i, "id"],
            "lowest_after_time": df.loc[post_low_i, "datetime"].strftime("%H:%M"),
            "lowest_after": round(float(df.loc[post_low_i, "low"]), 6),
            "highest_after_id": df.loc[post_high_i, "id"],
            "highest_after_time": df.loc[post_high_i, "datetime"].strftime("%H:%M"),
            "highest_after": round(float(df.loc[post_high_i, "high"]), 6),
            "mae_if_stop_anchor_high_plus_tick": round(max(0.0, float(after["high"].max()) - entry), 6),
            "mfe": round(entry - float(after["low"].min()), 6),
        },
    }

    print(json.dumps(out, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
