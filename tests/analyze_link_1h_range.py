import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from tools.config import PROJECT_ROOT, load_defaults
from tools.data_fetcher import FetchSpec, fetch_around_anchor, TZ_CN, _TF_MS
from tools.indicators import add_ema, add_wave_filter, classify_ema_stack
from tools.kline_features import add_kline_features
from tools.data_fetcher import _build_exchange
from tools.volume_profile import build_from_ohlcv

BASE = "LINK"
QUOTE = "USDT"
TIMEFRAME = "1h"
EXCHANGE = "binance"
MARKET = "futures"
START = pd.Timestamp("2026-05-08 17:00", tz=TZ_CN)
END = pd.Timestamp("2026-05-15 16:00", tz=TZ_CN)
OUT_DATE = "2026-05-15"
TRADE_ID = "range_LINK_1h_20260508_1700_20260515_1600"


def fetch_taker_orderflow(start_ms, end_ms):
    ex = _build_exchange(EXCHANGE, MARKET)
    symbol = f"{BASE}{QUOTE}"
    rows = []
    cursor = start_ms
    while cursor < end_ms:
        batch = ex.fapiPublicGetKlines({
            "symbol": symbol,
            "interval": TIMEFRAME,
            "startTime": cursor,
            "endTime": end_ms - 1,
            "limit": 1500,
        })
        if not batch:
            break
        rows.extend(batch)
        last_ts = int(batch[-1][0])
        if last_ts <= cursor:
            break
        cursor = last_ts + _TF_MS[TIMEFRAME]
        if len(batch) < 1500:
            break
    if not rows:
        return pd.DataFrame(columns=["timestamp", "buy_vol", "sell_vol", "total_trades_vol", "delta", "buy_ratio"])
    out = pd.DataFrame(rows)
    out = out[[0, 5, 9]].copy()
    out.columns = ["timestamp", "volume", "buy_vol"]
    out["timestamp"] = out["timestamp"].astype("int64")
    out["volume"] = out["volume"].astype("float64")
    out["buy_vol"] = out["buy_vol"].astype("float64")
    out = out.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    out["sell_vol"] = out["volume"] - out["buy_vol"]
    out["total_trades_vol"] = out["volume"]
    out["delta"] = out["buy_vol"] - out["sell_vol"]
    out["buy_ratio"] = out["buy_vol"] / out["total_trades_vol"]
    return out[["timestamp", "buy_vol", "sell_vol", "total_trades_vol", "delta", "buy_ratio"]]


def main():
    cfg = load_defaults()
    tf_ms = _TF_MS[TIMEFRAME]
    mid = START + (END - START) / 2
    anchor = mid.strftime("%Y-%m-%d %H:%M")
    bars_after = int((END - mid) / pd.Timedelta(tf_ms, unit="ms")) + 3
    spec = FetchSpec(
        exchange=EXCHANGE,
        market=MARKET,
        base=BASE,
        quote=QUOTE,
        timeframe=TIMEFRAME,
        anchor_cn=anchor,
        bars_before=cfg["data"]["warmup_bars"],
        bars_after=bars_after,
    )
    df = fetch_around_anchor(spec)
    add_ema(df, cfg["ema"]["periods"])
    wf = cfg["wave_filter"]
    add_wave_filter(
        df,
        rsi_period=wf["rsi_period"],
        stoch_period=wf["stoch_period"],
        smooth_k=wf["smooth_k"],
        smooth_d=wf["smooth_d"],
        ma_type=wf["ma_type"],
        source=wf["source"],
    )
    add_kline_features(df)
    df["ema_stack"] = df.apply(lambda r: classify_ema_stack(r, tuple(cfg["ema"]["periods"])), axis=1)

    start_ms = int(START.tz_convert("UTC").timestamp() * 1000)
    end_ms = int(END.tz_convert("UTC").timestamp() * 1000) + tf_ms
    win = df[(df["timestamp"] >= start_ms) & (df["timestamp"] < end_ms)].reset_index(drop=True).copy()
    win["id"] = [f"H{i:03d}" for i in range(len(win))]

    of = fetch_taker_orderflow(start_ms, end_ms)
    win = win.merge(of, on="timestamp", how="left")
    win[["buy_vol", "sell_vol", "total_trades_vol", "delta", "buy_ratio"]] = win[["buy_vol", "sell_vol", "total_trades_vol", "delta", "buy_ratio"]].fillna(0.0)
    win["cvd"] = win["delta"].cumsum()

    data_dir = PROJECT_ROOT / "reviews" / OUT_DATE / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    pq = data_dir / f"{TRADE_ID}.parquet"
    txt = data_dir / f"{TRADE_ID}.txt"
    win.to_parquet(pq, index=False)

    cols = ["id", "datetime", "open", "high", "low", "close", "volume", "vol_vs_ma", "wave", "ema_stack", "ema_21", "ema_55", "ema_100", "ema_200", "upper_wick_ratio", "lower_wick_ratio", "body_ratio", "engulf", "delta", "cvd", "buy_ratio"]
    win[cols].to_string(txt, index=False)

    vp = build_from_ohlcv(win, n_bins=80)

    thirds = []
    n = len(win)
    for name, sub in [("early", win.iloc[: n // 3]), ("middle", win.iloc[n // 3: 2 * n // 3]), ("late", win.iloc[2 * n // 3:])]:
        thirds.append({
            "part": name,
            "bars": len(sub),
            "open": float(sub["open"].iloc[0]),
            "close": float(sub["close"].iloc[-1]),
            "high": float(sub["high"].max()),
            "low": float(sub["low"].min()),
            "volume": float(sub["volume"].sum()),
            "delta": float(sub["delta"].sum()),
            "cvd_start": float(sub["cvd"].iloc[0]),
            "cvd_end": float(sub["cvd"].iloc[-1]),
            "buy_ratio_avg": float(sub["buy_ratio"].mean()),
            "close_change_pct": float((sub["close"].iloc[-1] / sub["open"].iloc[0] - 1) * 100),
        })

    summary = {
        "bars": len(win),
        "start": str(win["datetime"].iloc[0]),
        "end": str(win["datetime"].iloc[-1]),
        "open": float(win["open"].iloc[0]),
        "close": float(win["close"].iloc[-1]),
        "high": float(win["high"].max()),
        "high_id": str(win.loc[win["high"].idxmax(), "id"]),
        "high_time": str(win.loc[win["high"].idxmax(), "datetime"]),
        "low": float(win["low"].min()),
        "low_id": str(win.loc[win["low"].idxmin(), "id"]),
        "low_time": str(win.loc[win["low"].idxmin(), "datetime"]),
        "change_pct": float((win["close"].iloc[-1] / win["open"].iloc[0] - 1) * 100),
        "total_volume": float(win["volume"].sum()),
        "total_delta": float(win["delta"].sum()),
        "cvd_start": float(win["cvd"].iloc[0]),
        "cvd_end": float(win["cvd"].iloc[-1]),
        "buy_ratio_avg": float(win["buy_ratio"].mean()),
        "poc": float(vp.poc),
        "va_low": float(vp.va_low),
        "va_high": float(vp.va_high),
        "hvn_top": [float(x) for x in vp.hvn[:5]],
        "lvn_top": [float(x) for x in vp.lvn[:5]],
        "ema_stack_counts": win["ema_stack"].value_counts().to_dict(),
        "overbought_bars": int((win["wave"] >= 40).sum()),
        "oversold_bars": int((win["wave"] <= -40).sum()),
        "long_upper_wick_bars": int((win["upper_wick_ratio"] >= 0.4).sum()),
        "long_lower_wick_bars": int((win["lower_wick_ratio"] >= 0.4).sum()),
        "bear_engulf": int((win["engulf"] == "bear").sum()),
        "bull_engulf": int((win["engulf"] == "bull").sum()),
        "thirds": thirds,
        "top_volume_bars": win.sort_values("volume", ascending=False).head(12)[cols].to_dict("records"),
        "top_abs_delta_bars": win.assign(abs_delta=win["delta"].abs()).sort_values("abs_delta", ascending=False).head(12)[cols].to_dict("records"),
    }
    summary_path = data_dir / f"{TRADE_ID}.summary.json"
    pd.Series(summary, dtype="object").to_json(summary_path, force_ascii=False, indent=2)

    print(f"[saved] {pq.relative_to(PROJECT_ROOT)}")
    print(f"[saved] {txt.relative_to(PROJECT_ROOT)}")
    print(f"[saved] {summary_path.relative_to(PROJECT_ROOT)}")
    print(summary)


if __name__ == "__main__":
    main()
