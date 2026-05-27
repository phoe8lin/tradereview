import pandas as pd
from pathlib import Path
pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 30)

ROOT = Path("/Volumes/external/trade/交易复盘/reviews/2026-05-24/data")
df15 = pd.read_parquet(ROOT / "day_SOL_15m.parquet")
df5 = pd.read_parquet(ROOT / "supp_SOL_5m.parquet")

def show(df, lo, hi, label):
    sub = df[(df["datetime"] >= lo) & (df["datetime"] <= hi)].copy()
    sub["uw"] = sub["upper_wick_ratio"].round(3)
    sub["lw"] = sub["lower_wick_ratio"].round(3)
    sub["br"] = sub["body_ratio"].round(3)
    sub["wv"] = sub["wave"].round(2)
    sub["vmw"] = sub["vol_vs_ma"].round(2)
    sub["d"] = sub["delta"].round(0).astype("Int64")
    sub["cv"] = sub["cvd"].round(0).astype("Int64")
    sub["e200"] = sub["ema_200"].round(3)
    print(f"\n=== {label} ===")
    print(sub[["id","datetime","open","high","low","close","vmw","wv","uw","lw","br","engulf","d","cv","e200","ema_stack"]].to_string(index=False))

show(df15, "2026-05-24 02:30", "2026-05-24 06:30", "15m B10-B26")
show(df5,  "2026-05-24 04:00", "2026-05-24 05:45", "5m E12-E33")

# 关键评估
print("\n=== 评估关键数字 ===")
b18 = df15[df15["datetime"]=="2026-05-24 04:30"].iloc[0]
b19 = df15[df15["datetime"]=="2026-05-24 04:45"].iloc[0]
b20 = df15[df15["datetime"]=="2026-05-24 05:00"].iloc[0]
b21 = df15[df15["datetime"]=="2026-05-24 05:15"].iloc[0]
b22 = df15[df15["datetime"]=="2026-05-24 05:30"].iloc[0]
for b in [b18,b19,b20,b21,b22]:
    print(f"{b['id']} {b['datetime']} O{b['open']} H{b['high']} L{b['low']} C{b['close']} body_ratio={b['body_ratio']:.3f} uw={b['upper_wick_ratio']:.3f} lw={b['lower_wick_ratio']:.3f} Δ={b['delta']:.0f} CVD={b['cvd']:.0f} engulf='{b['engulf']}' wave={b['wave']:.2f}")

print(f"\n窗口前 (02:30 open → 04:30 open): {df15[df15['datetime']=='2026-05-24 02:30'].iloc[0]['open']:.3f} → {b18['open']:.3f}")
print(f"窗口高点 B20 H = {b20['high']:.3f}, 比窗口前高 {df15[(df15['datetime']>='2026-05-24 02:30') & (df15['datetime']<'2026-05-24 04:30')]['high'].max():.3f} 高 {b20['high'] - df15[(df15['datetime']>='2026-05-24 02:30') & (df15['datetime']<'2026-05-24 04:30')]['high'].max():.3f}")
print(f"05:30 前 1h Δ总 = {(b18['delta']+b19['delta']+b20['delta']+b21['delta']+b22['delta']):.0f}")
print(f"04:30-05:00 (B18-B20) Δ = {(b18['delta']+b19['delta']+b20['delta']):.0f}")
print(f"05:15-05:30 (B21-B22) Δ = {(b21['delta']+b22['delta']):.0f}")

# 后续 2h 行情验证 "持续回落至 85.80"
later = df15[(df15["datetime"]>="2026-05-24 05:30") & (df15["datetime"]<="2026-05-24 07:30")]
print(f"\n05:30-07:30 范围: low={later['low'].min():.3f} 出现于 {later.loc[later['low'].idxmin(),'datetime']} ({later.loc[later['low'].idxmin(),'id']})")
print(f"05:30 close→ 2h 后 low 跌幅 = {(later['low'].min()/b22['close']-1)*100:.2f}%")
print(f"05:00 high(87.510) → 2h 后 low 跌幅 = {(later['low'].min()/b20['high']-1)*100:.2f}%")
