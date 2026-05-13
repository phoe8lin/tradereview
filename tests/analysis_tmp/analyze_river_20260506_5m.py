"""RIVER 5m 2026-05-06 全天做空机会扫描"""
import pandas as pd
from pathlib import Path

df = pd.read_parquet("/Volumes/external/trade/交易复盘/reviews/2026-05-06/data/day_RIVER_5m.parquet")
df = df.set_index("id")

# 基础统计
print("=== 日概览 ===")
print(f"K线数: {len(df)}")
print(f"开盘: {df.iloc[0]['open']:.4f}  最新: {df.iloc[-1]['close']:.4f}")
print(f"日内高: {df['high'].max():.4f}  日内低: {df['low'].min():.4f}")
print(f"振幅: {(df['high'].max()/df['low'].min()-1)*100:.2f}%")
stacks = df['ema_stack'].value_counts()
print(f"EMA排列: {dict(stacks)}")
print(f"全日delta合计: {df['delta'].sum():.0f}")
print(f"CVD: {df.iloc[0]['cvd']:.0f} → {df.iloc[-1]['cvd']:.0f}")
print(f"buy_ratio均值: {df['buy_ratio'].mean():.3f}")

# EMA轨迹
print("\n=== EMA200轨迹 ===")
for i in [0, 24, 48, 72, 96, 120, 144, 168, -1]:
    r = df.iloc[i]
    t = str(r['datetime'])[-8:-3]
    print(f"  {df.index[i]} {t} EMA200={r['ema_200']:.4f}")

# 识别回踩均线+拒绝序列
print("\n=== 回踩EMA21拒绝序列 (bear_stack only) ===")
seqs = []
for i in range(3, len(df)):
    r0 = df.iloc[i-3]
    r1 = df.iloc[i-2]
    r2 = df.iloc[i-1]
    r3 = df.iloc[i]  # 拒绝K
    
    if r3['ema_stack'] != 'bear_stack':
        continue
    
    # 前3根有反弹: 至少一根阳线或high抬升
    bounce_high = max(r0['high'], r1['high'], r2['high'])
    bounce_low = min(r0['low'], r1['low'], r2['low'])
    bounce_range = bounce_high - bounce_low
    
    # 反弹接近EMA21
    near_ema = abs(bounce_high - r3['ema_21']) / r3['ema_21'] < 0.006
    
    # 拒绝K: 阴线 + (长上影 或 bear engulf 或 强实体)
    is_reject = (r3['close'] < r3['open'] and 
                (r3['upper_wick_ratio'] >= 0.35 or r3['engulf'] == 'bear' or r3['body_ratio'] >= 0.5))
    
    if near_ema and is_reject:
        idx = df.index[i]
        t = str(r3['datetime'])[-8:-3]
        seqs.append({
            'id': idx, 'time': t,
            'bounce_high': bounce_high, 'ema21': r3['ema_21'],
            'reject_c': r3['close'], 'reject_o': r3['open'],
            'upper_wick': r3['upper_wick_ratio'],
            'body': r3['body_ratio'],
            'engulf': r3['engulf'],
            'delta': r3['delta'], 'vol': r3['vol_vs_ma'],
            'wave': r3['wave'],
            'buy_ratio': r3['buy_ratio'],
        })

print(f"找到 {len(seqs)} 个回踩EMA21拒绝序列:\n")
for s in seqs:
    print(f"  {s['id']} {s['time']} | 反弹高={s['bounce_high']:.4f} EMA21={s['ema21']:.4f}")
    print(f"    拒绝: O={s['reject_o']:.4f}→C={s['reject_c']:.4f} body={s['body']:.3f} upper_wick={s['upper_wick']:.3f} engulf={s['engulf']}")
    print(f"    delta={s['delta']:.0f} buy_ratio={s['buy_ratio']:.3f} vol={s['vol']:.2f}x wave={s['wave']:.1f}")
    print()

# 放量暴跌K线
print("\n=== 放量暴跌K线 (vol>=3x, delta<-5000) ===")
for idx in df.index:
    r = df.loc[idx]
    if r['vol_vs_ma'] >= 3 and r['delta'] < -5000:
        t = str(r['datetime'])[-8:-3]
        print(f"  {idx} {t} vol={r['vol_vs_ma']:.1f}x delta={r['delta']:.0f} O={r['open']:.4f} C={r['close']:.4f}")

# Wave极值
print("\n=== Wave极值 ===")
wmax = df.loc[df['wave'].idxmax()]
wmin = df.loc[df['wave'].idxmin()]
print(f"  MAX: {df['wave'].idxmax()} wave={wmax['wave']:.1f} time={str(wmax['datetime'])[-8:-3]}")
print(f"  MIN: {df['wave'].idxmin()} wave={wmin['wave']:.1f} time={str(wmin['datetime'])[-8:-3]}")
