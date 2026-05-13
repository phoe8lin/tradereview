"""HYPE 5m 2026-05-06 16:05 长上影线做空分析"""
import pandas as pd

df = pd.read_parquet("/Volumes/external/trade/交易复盘/reviews/2026-05-06/data/day_HYPE_5m.parquet")
df = df.set_index("id")

def show_range(start, end, title):
    print(f"\n--- {title} ---")
    subset = df.loc[start:end]
    for idx in subset.index:
        r = subset.loc[idx]
        t = str(r['datetime'])[-8:-3]
        print(f"  {idx} {t} O={r['open']:.4f} H={r['high']:.4f} L={r['low']:.4f} C={r['close']:.4f} "
              f"E21={r['ema_21']:.4f} E200={r['ema_200']:.4f} {r['ema_stack']} "
              f"body={r['body_ratio']:.3f} uw={r['upper_wick_ratio']:.3f} lw={r['lower_wick_ratio']:.3f} "
              f"delta={r['delta']:.0f} vol={r['vol_vs_ma']:.2f}x wave={r['wave']:.1f} engulf={r['engulf']}")

# 1. 早盘高点区域
show_range("B58", "B66", "早盘高点: B60-B61 (05:00-05:10) 高点44.620")

# 2. 午后拉升到16:05
show_range("B173", "B196", "午后拉升→16:05冲高回落: B173-B196")

# 3. 关键统计
print("\n" + "=" * 100)
print("关键数据对比")
print("=" * 100)

b61 = df.loc["B61"]
b193 = df.loc["B193"]
print(f"\nB61 (05:05) 早盘高点: H={b61['high']:.4f}  C={b61['close']:.4f}  vol={b61['vol_vs_ma']:.2f}x  delta={b61['delta']:.0f}")
print(f"B193 (16:05) 午后冲高: H={b193['high']:.4f}  C={b193['close']:.4f}  vol={b193['vol_vs_ma']:.2f}x  delta={b193['delta']:.0f}")
print(f"  高点差: {b193['high'] - b61['high']:.4f} (仅高 {b193['high'] - b61['high']:.4f})")

# 4. B193前后delta流
print("\n【B193 前后 delta 流分析】")
for bid in ["B190","B191","B192","B193","B194","B195","B196"]:
    r = df.loc[bid]
    t = str(r['datetime'])[-8:-3]
    print(f"  {bid} {t} delta={r['delta']:+.0f}  CVD={r['cvd']:.0f}  vol={r['vol_vs_ma']:.2f}x")

# 5. 14:45-16:05 累计
subset = df.loc["B177":"B193"]
cum_delta = subset['delta'].sum()
print(f"\nB177-B193 (14:45→16:05) 累计delta: {cum_delta:+.0f}")
print(f"  CVD变化: {df.loc['B177','cvd']:.0f} → {df.loc['B193','cvd']:.0f}")

# 6. B193 之后
post = df.loc["B194":"B196"]
print(f"\nB194-B196 (16:10-16:20) 累计delta: {post['delta'].sum():+.0f}")
print(f"  价格: {b193['close']:.4f} → {df.loc['B196','close']:.4f} (跌幅 {(df.loc['B196','close']/b193['close']-1)*100:.2f}%)")

# 7. Wave 状态
print(f"\nB193 wave={b193['wave']:.1f} (未超买, 阈值±40)")
print(f"B192 wave={df.loc['B192','wave']:.1f} → B193 wave={b193['wave']:.1f}")

# 8. EMA 关系
print(f"\nB193 EMA: E21={b193['ema_21']:.4f} E200={b193['ema_200']:.4f} 价差={b193['close']-b193['ema_200']:.4f}")
print(f"  B193 high vs EMA200: {b193['high']-b193['ema_200']:.4f} (溢价 {b193['high']/b193['ema_200']*100-100:.2f}%)")
