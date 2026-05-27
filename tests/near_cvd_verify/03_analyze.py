"""
步骤 3：综合分析
- 真实连续 perp/spot CVD vs 价格的关系
- 资金费率方向 + 累计成本
- 分阶段订单流真相 (Phase1 / 2a / 2b)
- 阳/阴线量比（真值）
- 02-25 暴涨日真相核验
- OI / top trader LSR 在突破段的方向
- 与原报告核对差异
"""
from __future__ import annotations

from pathlib import Path
import pandas as pd

DATA = Path(__file__).resolve().parent / "data"
OUT = Path(__file__).resolve().parent

perp = pd.read_parquet(DATA / "near_perp_daily_full.parquet")
spot = pd.read_parquet(DATA / "near_spot_daily_full.parquet")
fr = pd.read_parquet(DATA / "near_funding.parquet")
fr_d = pd.read_csv(DATA / "near_funding_daily.csv")
oi_1d = pd.read_parquet(DATA / "near_oi_1d.parquet")
lsr = pd.read_parquet(DATA / "near_top_lsr.parquet")


def phase(d: str) -> str:
    if d <= "2026-02-23":
        return "Phase1"
    if d <= "2026-03-05":
        return "Phase2a"
    if d <= "2026-03-11":
        return "GAP"
    return "Phase2b"


for df in (perp, spot):
    df["phase"] = df["date_utc"].apply(phase)
    df["is_up"] = df["close"] > df["open"]


print("=" * 78)
print("【1】真实连续 CVD —— 全期累计")
print("=" * 78)
total_perp = perp["delta"].sum()
total_spot = spot["delta"].sum()
print(f"perp 全期累计 delta = {total_perp:>+15,.0f}")
print(f"spot 全期累计 delta = {total_spot:>+15,.0f}")
print(f"perp CVD 起点 0 → 末值 {perp['cvd'].iloc[-1]:+,.0f}")
print(f"spot CVD 起点 0 → 末值 {spot['cvd'].iloc[-1]:+,.0f}")

print()
print("=" * 78)
print("【2】分阶段聚合（perp）")
print("=" * 78)
agg_perp = perp.groupby("phase").agg(
    days=("date_utc", "count"),
    open_first=("open", "first"),
    close_last=("close", "last"),
    vol_sum=("volume", "sum"),
    delta_sum=("delta", "sum"),
    buy_ratio_w=("taker_buy_base", "sum"),
    sell_w=("taker_sell_base", "sum"),
).reset_index()
agg_perp["pct_change"] = (agg_perp["close_last"] / agg_perp["open_first"] - 1) * 100
agg_perp["buy_ratio_pct"] = agg_perp["buy_ratio_w"] / agg_perp["vol_sum"] * 100
print(agg_perp[["phase", "days", "open_first", "close_last", "pct_change",
                "vol_sum", "delta_sum", "buy_ratio_pct"]].to_string(index=False))

print()
print("【2b】分阶段聚合（spot）")
agg_spot = spot.groupby("phase").agg(
    days=("date_utc", "count"),
    open_first=("open", "first"),
    close_last=("close", "last"),
    vol_sum=("volume", "sum"),
    delta_sum=("delta", "sum"),
    buy_ratio_w=("taker_buy_base", "sum"),
).reset_index()
agg_spot["pct_change"] = (agg_spot["close_last"] / agg_spot["open_first"] - 1) * 100
agg_spot["buy_ratio_pct"] = agg_spot["buy_ratio_w"] / agg_spot["vol_sum"] * 100
print(agg_spot[["phase", "days", "open_first", "close_last", "pct_change",
                "vol_sum", "delta_sum", "buy_ratio_pct"]].to_string(index=False))

print()
print("=" * 78)
print("【3】Phase 2b 阳/阴线量比（真值，全 78 天）")
print("=" * 78)
p2b = perp[perp["phase"] == "Phase2b"]
up = p2b[p2b["is_up"]]; dn = p2b[~p2b["is_up"]]
print(f"阳线 {len(up)} 天，日均量 {up['volume'].mean()/1e6:.2f}M，"
      f"日均 delta {up['delta'].mean():>+,.0f}，加权 buy% {up['taker_buy_base'].sum()/up['volume'].sum()*100:.2f}")
print(f"阴线 {len(dn)} 天，日均量 {dn['volume'].mean()/1e6:.2f}M，"
      f"日均 delta {dn['delta'].mean():>+,.0f}，加权 buy% {dn['taker_buy_base'].sum()/dn['volume'].sum()*100:.2f}")
print(f"阴/阳量比 = {dn['volume'].mean()/up['volume'].mean():.3f}")

print()
print("=" * 78)
print("【4】资金费率（每 8h 一次，三段累计 + 日均）")
print("=" * 78)
fr["date_utc"] = pd.to_datetime(fr["fundingTime"], unit="ms", utc=True).dt.strftime("%Y-%m-%d")
fr["phase"] = fr["date_utc"].apply(phase)
agg_fr = fr.groupby("phase")["fundingRate"].agg(["count", "mean", "sum", "min", "max"]).reset_index()
agg_fr["mean_bps"] = agg_fr["mean"] * 10000
agg_fr["sum_pct"] = agg_fr["sum"] * 100
print(agg_fr[["phase", "count", "mean_bps", "sum_pct", "min", "max"]].to_string(index=False))
print("（mean_bps = 单次平均 funding 万分之；sum_pct = 累计费率 %，多头长期持仓总成本）")

print()
print("=" * 78)
print("【5】02-25 暴涨日真相 —— 与原报告对照")
print("=" * 78)
for d in ["2026-02-24", "2026-02-25", "2026-02-26"]:
    pr = perp[perp["date_utc"] == d].iloc[0]
    sp = spot[spot["date_utc"] == d].iloc[0]
    print(f"{d}")
    print(f"  perp open={pr['open']:.3f} close={pr['close']:.3f} ({(pr['close']/pr['open']-1)*100:+.1f}%) "
          f"vol={pr['volume']/1e6:.1f}M delta={pr['delta']:>+,.0f} buy%={pr['buy_ratio']*100:.2f}")
    print(f"  spot open={sp['open']:.3f} close={sp['close']:.3f} ({(sp['close']/sp['open']-1)*100:+.1f}%) "
          f"vol={sp['volume']/1e6:.1f}M delta={sp['delta']:>+,.0f} buy%={sp['buy_ratio']*100:.2f}")

print()
print("=" * 78)
print("【6】原报告 23 个采样日 vs 真实值对照")
print("=" * 78)
samples = {
    "2026-02-25": ("Phase2a 暴涨日", 321248, 0.502),
    "2026-02-26": ("Phase2a", 1942782, 0.506),
    "2026-03-02": ("Phase2a 大阳", 1836136, 0.506),
    "2026-04-09": ("Phase2b 价涨Δ大负", -2677780, 0.485),
    "2026-04-16": ("Phase2b 买方回归", 745137, 0.505),
    "2026-04-23": ("Phase2b 卖压", -1286744, 0.487),
    "2026-04-30": ("Phase2b 缩量阴跌", -1677803, 0.480),
    "2026-05-07": ("Phase2b 买方", 839668, 0.503),
}
print(f"{'date':<12}{'note':<22}{'orig_Δ':>14}{'true_Δ':>14}{'orig_buy%':>11}{'true_buy%':>11}")
for d, (note, od, ob) in samples.items():
    pr = perp[perp["date_utc"] == d]
    if pr.empty:
        continue
    pr = pr.iloc[0]
    print(f"{d:<12}{note:<22}{od:>+14,.0f}{pr['delta']:>+14,.0f}"
          f"{ob*100:>10.1f}%{pr['buy_ratio']*100:>10.2f}%")

print()
print("=" * 78)
print("【7】Phase 2b 修正后日 delta 分布")
print("=" * 78)
p2b_pos = (p2b["delta"] > 0).sum()
p2b_neg = (p2b["delta"] < 0).sum()
print(f"Phase2b 共 {len(p2b)} 天，delta>0 {p2b_pos} 天，delta<0 {p2b_neg} 天")
print(f"Phase2b 累计 delta（perp） = {p2b['delta'].sum():+,.0f}")
print(f"Phase2b 累计 delta（spot） = {spot[spot['phase']=='Phase2b']['delta'].sum():+,.0f}")
print()
print("Phase2b 内部细分（按月）:")
p2b_m = p2b.copy()
p2b_m["mon"] = p2b_m["date_utc"].str[:7]
mon_perp = p2b_m.groupby("mon").agg(
    days=("date_utc", "count"),
    px_first=("open", "first"),
    px_last=("close", "last"),
    delta=("delta", "sum"),
    vol=("volume", "sum"),
    buy_w=("taker_buy_base", "sum"),
).reset_index()
mon_perp["chg%"] = (mon_perp["px_last"]/mon_perp["px_first"]-1)*100
mon_perp["buy%"] = mon_perp["buy_w"]/mon_perp["vol"]*100
print(mon_perp[["mon","days","px_first","px_last","chg%","delta","buy%"]].to_string(index=False))

print()
print("=" * 78)
print("【8】OI 与 top trader 多空比（近 30 天）")
print("=" * 78)
oi_1d_sorted = oi_1d.sort_values("timestamp")
print("OI（合约张数）:")
print(oi_1d_sorted[["date_utc","sumOpenInterest","sumOpenInterestValue"]].to_string(index=False))
print()
print("Top trader 账户多空比:")
print(lsr[["date_utc","longShortRatio","longAccount","shortAccount"]].sort_values("date_utc").to_string(index=False))

print()
print("=" * 78)
print("【9】最近 6 天爆发段订单流细节")
print("=" * 78)
recent = perp[perp["date_utc"] >= "2026-05-17"][["date_utc","open","close","volume","delta","buy_ratio"]].copy()
recent_s = spot[spot["date_utc"] >= "2026-05-17"][["date_utc","close","volume","delta","buy_ratio"]].copy()
m = recent.merge(recent_s, on="date_utc", suffixes=("_perp","_spot"))
m["chg%"] = (m["close_perp"]/m["open"]-1)*100
print(m.to_string(index=False))
