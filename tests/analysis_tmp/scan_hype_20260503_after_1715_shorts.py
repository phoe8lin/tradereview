from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.validators import load_day, parse_ref, detect_features

DATE = "2026-05-03"
SYMBOL = "HYPE"
TF = "5m"
RR = 1.30
BUFFER = 0.019
START = "B207"
END = "23:00"


def signal_tags(row):
    feats = detect_features(row).patterns
    tags = []
    if "engulf_bear" in feats:
        tags.append("bear_engulf")
    if "high_volume" in feats:
        tags.append("high_vol")
    if "pin_bar_top" in feats:
        tags.append("pin_top")
    if "long_upper_wick" in feats:
        tags.append("long_upper")
    if float(row["delta"]) < -1500:
        tags.append("delta_neg")
    if float(row["wave"]) >= 40:
        tags.append("overbought")
    if float(row["wave"]) <= -40:
        tags.append("oversold")
    if float(row["close"]) < float(row["ema_200"]) and float(row["high"]) >= float(row["ema_200"]):
        tags.append("ema200_reject")
    if str(row["ema_stack"]) == "bear_stack":
        tags.append("bear_stack")
    return tags


def evaluate(df, idx, stop_mode):
    row = df.iloc[idx]
    entry = float(row["close"])
    if stop_mode == "bar_high":
        stop_base = float(row["high"])
    elif stop_mode == "prev3_swing":
        stop_base = float(df.iloc[max(0, idx - 2):idx + 1]["high"].max())
    elif stop_mode == "prev6_swing":
        stop_base = float(df.iloc[max(0, idx - 5):idx + 1]["high"].max())
    else:
        raise ValueError(stop_mode)
    stop = round(stop_base + BUFFER, 3)
    risk = stop - entry
    if risk <= 0:
        return None
    take = round(entry - risk * RR, 3)

    after = df.iloc[idx + 1:parse_ref(df, END) + 1]
    outcome = "NEITHER"
    fire_id = "-"
    fire_time = "-"
    bars = None
    for j, r in after.iterrows():
        hit_stop = float(r["high"]) >= stop
        hit_take = float(r["low"]) <= take
        if hit_stop or hit_take:
            outcome = "STOP" if hit_stop else "TAKE"
            if hit_stop and hit_take:
                outcome = "AMBIG_STOP_FIRST"
            fire_id = r["id"]
            fire_time = pd.to_datetime(r["datetime"]).strftime("%H:%M")
            bars = j - idx
            break
    mae = float(after["high"].max()) - entry if not after.empty else 0.0
    mfe = entry - float(after["low"].min()) if not after.empty else 0.0
    return {
        "id": row["id"],
        "time": pd.to_datetime(row["datetime"]).strftime("%H:%M"),
        "mode": stop_mode,
        "entry": round(entry, 3),
        "stop": stop,
        "risk": round(risk, 3),
        "take": take,
        "rr": RR,
        "outcome": outcome,
        "fire": f"{fire_id} {fire_time}",
        "bars": bars,
        "mae_r": round(mae / risk, 2),
        "mfe_r": round(mfe / risk, 2),
        "H": round(float(row["high"]), 3),
        "L": round(float(row["low"]), 3),
        "C": round(float(row["close"]), 3),
        "volx": round(float(row["vol_vs_ma"]), 2),
        "wave": round(float(row["wave"]), 1),
        "delta": round(float(row["delta"]), 0),
        "stack": row["ema_stack"],
        "tags": "+".join(signal_tags(row)),
    }


def main():
    df = load_day(DATE, SYMBOL, TF)
    start_i = parse_ref(df, START)
    end_i = parse_ref(df, END)
    rows = []
    for idx in range(start_i, end_i + 1):
        row = df.iloc[idx]
        tags = signal_tags(row)
        if idx == start_i or "bear_engulf" in tags or "high_vol" in tags or "pin_top" in tags or ("delta_neg" in tags and "long_upper" in tags):
            for mode in ["bar_high", "prev3_swing", "prev6_swing"]:
                ev = evaluate(df, idx, mode)
                if ev:
                    rows.append(ev)
    out = pd.DataFrame(rows)
    print("ALL_CANDIDATES")
    print(out.to_string(index=False))
    print("\nTAKE_ONLY_SORTED")
    take = out[out["outcome"] == "TAKE"].copy()
    if not take.empty:
        take["score"] = 0
        take.loc[take["mae_r"] <= 0.35, "score"] += 2
        take.loc[take["mae_r"] <= 0.6, "score"] += 1
        take.loc[take["bars"] <= 24, "score"] += 2
        take.loc[take["risk"] <= 0.12, "score"] += 1
        take.loc[take["tags"].str.contains("bear_stack", regex=False), "score"] += 1
        take.loc[take["tags"].str.contains("high_vol", regex=False), "score"] += 1
        take.loc[take["tags"].str.contains("delta_neg", regex=False), "score"] += 1
        print(take.sort_values(["score", "mae_r", "bars"], ascending=[False, True, True]).to_string(index=False))


if __name__ == "__main__":
    main()
