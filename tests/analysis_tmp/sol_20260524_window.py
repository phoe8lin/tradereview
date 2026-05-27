"""核实 SOL 5/24 04:30-05:30 BJ 窗口（15m + 5m）的真实数据，
评估其他 agent 提出的"死猫跳"判定是否成立。"""
import pandas as pd
from pathlib import Path

ROOT = Path("/Volumes/external/trade/交易复盘/reviews/2026-05-24/data")
df15 = pd.read_parquet(ROOT / "day_SOL_15m.parquet")
df5 = pd.read_parquet(ROOT / "supp_SOL_5m.parquet")

cols = ["id", "datetime", "open", "high", "low", "close", "volume", "vol_vs_ma",
        "wave", "upper_wick_ratio", "lower_wick_ratio", "body_ratio", "engulf",
        "delta", "cvd", "buy_ratio", "ema_55", "ema_100", "ema_200", "ema_stack"]

print("=== 15m 主周期 B16-B26 (03:45-06:30) ===")
m15 = df15[(df15["datetime"] >= "2026-05-24 03:45") & (df15["datetime"] <= "2026-05-24 06:30")]
print(m15[cols].to_string(index=False))

print()
print("=== 5m 辅周期 04:00-05:45 ===")
m5 = df5[(df5["datetime"] >= "2026-05-24 04:00") & (df5["datetime"] <= "2026-05-24 05:45")]
print(m5[cols].to_string(index=False))

print()
print("=== 窗口前 2h 走势 (02:30-04:30 15m, 用于核验“前期趋势”) ===")
pre = df15[(df15["datetime"] >= "2026-05-24 02:30") & (df15["datetime"] < "2026-05-24 04:30")]
print(pre[["id","datetime","open","high","low","close","volume","vol_vs_ma","delta","cvd","ema_200","ema_stack"]].to_string(index=False))

print()
print("=== 关键指标 ===")
print(f"04:30 (B18) 之前 2h 价格区间: low={pre['low'].min():.3f} high={pre['high'].max():.3f} "
      f"open(02:30)={pre.iloc[0]['open']:.3f} close(04:15)={pre.iloc[-1]['close']:.3f}")
top_bar = m15.loc[m15["high"].idxmax()]
print(f"窗口最高: {top_bar['id']} {top_bar['datetime']} high={top_bar['high']:.3f}")
end_bar = m15[m15["datetime"] == "2026-05-24 05:30"].iloc[0]
print(f"05:30 (B22) close={end_bar['close']:.3f}")
print(f"高点回撤: {top_bar['high']:.3f} -> {end_bar['close']:.3f} = {(end_bar['close']/top_bar['high']-1)*100:.2f}%")

print()
print("=== Δ 前后半段对比 ===")
first_half = m15[(m15["datetime"] >= "2026-05-24 04:30") & (m15["datetime"] <= "2026-05-24 05:00")]
second_half = m15[(m15["datetime"] > "2026-05-24 05:00") & (m15["datetime"] <= "2026-05-24 05:30")]
print(f"04:30-05:00 累计 Δ = {first_half['delta'].sum():,.0f}")
print(f"05:15-05:30 累计 Δ = {second_half['delta'].sum():,.0f}")
