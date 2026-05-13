from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.validators import load_day, get_bar, parse_ref, detect_features, ema_relation, validate_trade
from tools.validators.ema_regime import regime_summary, regime_segments

DATE = "2026-05-03"
SYMBOL = "HYPE"
TF = "5m"
ENTRY_BAR = "B207"
SIDE = "short"
ENTRY = 41.139
STOP = 41.315
RR = 1.30
TAKE = ENTRY - (STOP - ENTRY) * RR


def fmt_bar(row):
    feats = detect_features(row).patterns
    return {
        "id": row["id"],
        "time": pd.to_datetime(row["datetime"]).strftime("%H:%M"),
        "O": round(float(row["open"]), 3),
        "H": round(float(row["high"]), 3),
        "L": round(float(row["low"]), 3),
        "C": round(float(row["close"]), 3),
        "volx": round(float(row["vol_vs_ma"]), 3),
        "ema21": round(float(row["ema_21"]), 3),
        "ema55": round(float(row["ema_55"]), 3),
        "ema100": round(float(row["ema_100"]), 3),
        "ema200": round(float(row["ema_200"]), 3),
        "stack": row["ema_stack"],
        "wave": round(float(row["wave"]), 3),
        "body": round(float(row["body_ratio"]), 3),
        "upW": round(float(row["upper_wick_ratio"]), 3),
        "loW": round(float(row["lower_wick_ratio"]), 3),
        "engulf": row.get("engulf", ""),
        "delta": round(float(row["delta"]), 0),
        "cvd": round(float(row["cvd"]), 0),
        "buy_ratio": round(float(row["buy_ratio"]), 3),
        "features": ",".join(feats),
    }


def first_touch_after(df, start_ref, entry, stop, take):
    start_i = parse_ref(df, start_ref)
    rows = []
    for i in range(start_i + 1, len(df)):
        row = df.iloc[i]
        h = float(row["high"])
        l = float(row["low"])
        touched_stop = h >= stop
        touched_take = l <= take
        rows.append((i, row, touched_stop, touched_take))
        if touched_stop or touched_take:
            return rows, (i, row, touched_stop, touched_take)
    return rows, None


def main():
    df = load_day(DATE, SYMBOL, TF)
    i0 = parse_ref(df, "16:40")
    i1 = parse_ref(df, "23:00")
    win = df.iloc[i0:i1 + 1].copy()

    print(f"DATA={df.attrs['source_path']}")
    print(f"WINDOW {win.iloc[0]['id']} {win.iloc[0]['datetime']} -> {win.iloc[-1]['id']} {win.iloc[-1]['datetime']} n={len(win)}")
    print("REGIME_SUMMARY", regime_summary(df))
    print("REGIME_SEGMENTS_16_23")
    for seg in regime_segments(df):
        if seg.end_time >= "16:40" and seg.start_time <= "23:00":
            print(seg)

    print("\nKEY B200-B210")
    print(pd.DataFrame([fmt_bar(df.iloc[i]) for i in range(parse_ref(df, "B200"), parse_ref(df, "B210") + 1)]).to_string(index=False))

    print("\nENTRY_BAR")
    b = get_bar(df, ENTRY_BAR)
    print(pd.DataFrame([fmt_bar(b)]).to_string(index=False))
    print("EMA_REL", ema_relation(b))
    print("FEATURES", detect_features(b).to_dict())

    print("\nTRADE_VALIDATE")
    vr = validate_trade(df, ENTRY_BAR, SIDE, ENTRY, STOP, TAKE, claimed_rr=RR, horizon=80)
    print("take", round(TAKE, 4), "overall", vr["overall"])
    for k, v in vr["checks"].items():
        print(k, v)

    rows, touch = first_touch_after(df, ENTRY_BAR, ENTRY, STOP, TAKE)
    sub = df.iloc[parse_ref(df, ENTRY_BAR) + 1:parse_ref(df, "23:00") + 1]
    max_high_idx = sub["high"].idxmax()
    min_low_idx = sub["low"].idxmin()
    print("\nPATH_TO_23")
    print("max_high", df.loc[max_high_idx, "id"], df.loc[max_high_idx, "datetime"], float(df.loc[max_high_idx, "high"]), "adverse", round(float(df.loc[max_high_idx, "high"]) - ENTRY, 4), "R", round((float(df.loc[max_high_idx, "high"]) - ENTRY)/(STOP-ENTRY), 2))
    print("min_low", df.loc[min_low_idx, "id"], df.loc[min_low_idx, "datetime"], float(df.loc[min_low_idx, "low"]), "favorable", round(ENTRY - float(df.loc[min_low_idx, "low"]), 4), "R", round((ENTRY - float(df.loc[min_low_idx, "low"]))/(STOP-ENTRY), 2))
    if touch:
        i, row, ts, tt = touch
        print("first_touch", row["id"], row["datetime"], "stop", ts, "take", tt, "H", float(row["high"]), "L", float(row["low"]), "bars", i - parse_ref(df, ENTRY_BAR))

    print("\nIMPORTANT_EVENTS")
    events = []
    for _, row in win.iterrows():
        feats = detect_features(row).patterns
        if row["id"] in ["B200", "B205", "B206", "B207", "B213", "B238", "B260", "B268", "B272", "B273", "B275", "B276"] or feats or abs(float(row["wave"])) >= 40 or float(row["vol_vs_ma"]) >= 1.8:
            events.append(fmt_bar(row))
    print(pd.DataFrame(events).to_string(index=False))


if __name__ == "__main__":
    main()
