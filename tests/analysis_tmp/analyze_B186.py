"""分析 B186 (15:30) 做空设置"""
import pandas as pd
df = pd.read_parquet("/Volumes/external/trade/交易复盘/reviews/2026-05-06/data/day_RIVER_5m.parquet")
df = df.set_index("id")

# B165-B191 详细
print("=" * 80)
print("B186 (15:30) 做空分析")
print("=" * 80)

# 入场K
r = df.loc["B186"]
print(f"\n入场K B186: O={r['open']:.4f} H={r['high']:.4f} L={r['low']:.4f} C={r['close']:.4f}")
print(f"EMA21={r['ema_21']:.4f} EMA55={r['ema_55']:.4f} EMA200={r['ema_200']:.4f} {r['ema_stack']}")
print(f"body={r['body_ratio']:.3f} uw={r['upper_wick_ratio']:.3f} lw={r['lower_wick_ratio']:.3f}")
print(f"delta={r['delta']:.0f} vol={r['vol_vs_ma']:.2f}x wave={r['wave']:.1f} engulf={r['engulf']}")
print(f"buy_ratio={r['buy_ratio']:.3f}")

# 前置K B185
r185 = df.loc["B185"]
print(f"\n前置K B185: O={r185['open']:.4f} H={r185['high']:.4f} C={r185['close']:.4f}")
print(f"body={r185['body_ratio']:.3f} delta={r185['delta']:.0f} buy_ratio={r185['buy_ratio']:.3f}")

# 14:00以来EMA21测试
print("\n=== 14:00以来EMA21测试记录 ===")
for idx in df.index[168:187]:  # B168-B186
    rk = df.loc[idx]
    t = str(rk['datetime'])[-8:-3]
    dist_pct = (rk['high'] - rk['ema_21']) / rk['ema_21'] * 100
    touch = "✅ 触及" if rk['high'] >= rk['ema_21'] else f"差{dist_pct:.2f}%"
    print(f"  {idx} {t} H={rk['high']:.4f} vs EMA21={rk['ema_21']:.4f} {touch}")

# 低点序列
print("\n=== 低点序列 ===")
lows = []
for idx in df.index[168:187]:
    rk = df.loc[idx]
    lows.append((idx, str(rk['datetime'])[-8:-3], rk['low']))
    print(f"  {idx} {str(rk['datetime'])[-8:-3]} L={rk['low']:.4f}")

# 盈亏比计算
entry = 5.694
stop = 5.732
risk = stop - entry
print(f"\n=== 盈亏比计算 ===")
print(f"入场: {entry:.4f}  止损: {stop:.4f}  风险: {risk:.4f}")
for rr in [1.0, 1.3, 1.5, 2.0]:
    target = entry - rr * risk
    print(f"  RR={rr:.1f} → 目标={target:.4f}")

# 后验
print(f"\n=== 后验 (B186之后) ===")
for idx in df.index[187:]:
    rk = df.loc[idx]
    t = str(rk['datetime'])[-8:-3]
    profit = entry - rk['low']
    rr_actual = profit / risk
    print(f"  {idx} {t} L={rk['low']:.4f} 最大盈利={profit:.4f} RR={rr_actual:.2f}")

# 最近参考低点
print(f"\n=== 参考支撑位 ===")
refs = [
    ("B176 low", df.loc["B176"]["low"]),
    ("B181 low", df.loc["B181"]["low"]),
    ("B182 low", df.loc["B182"]["low"]),
    ("B156 low (日低)", df.loc["B156"]["low"]),
    ("B188 low", df.loc["B188"]["low"]),
    ("B189 low", df.loc["B189"]["low"]),
]
for name, val in refs:
    rr_to_ref = (entry - val) / risk
    print(f"  {name}: {val:.4f}  RR={rr_to_ref:.2f}")
