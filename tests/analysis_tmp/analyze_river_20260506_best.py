"""精选 RIVER 5m 2026-05-06 最佳回踩EMA21做空机会"""
import pandas as pd

df = pd.read_parquet("/Volumes/external/trade/交易复盘/reviews/2026-05-06/data/day_RIVER_5m.parquet")
df = df.set_index("id")

# 手动标注关键波段
print("=" * 90)
print("RIVER/USDT 5m · 2026-05-06 · 精选回踩EMA21做空机会")
print("=" * 90)

# 打印关键区间数据
def show_range(start, end, title):
    print(f"\n--- {title} ---")
    subset = df.loc[start:end]
    for idx in subset.index:
        r = subset.loc[idx]
        t = str(r['datetime'])[-8:-3]
        print(f"  {idx} {t} O={r['open']:.4f} H={r['high']:.4f} L={r['low']:.4f} C={r['close']:.4f} "
              f"E21={r['ema_21']:.4f} E200={r['ema_200']:.4f} {r['ema_stack']} "
              f"body={r['body_ratio']:.3f} uw={r['upper_wick_ratio']:.3f} "
              f"delta={r['delta']:.0f} vol={r['vol_vs_ma']:.2f}x wave={r['wave']:.1f} "
              f"engulf={r['engulf']}")

# 1. 凌晨反弹拒绝
show_range("B10", "B17", "机会1: B15 凌晨反弹至EMA21后拒绝")

# 2. 早盘破位 
show_range("B92", "B100", "机会2: B96-B97 早盘EMA21拒绝+放量暴跌")

# 3. 午前暴跌
show_range("B127", "B137", "机会3: B134-B135 午前反弹衰竭+放量暴跌")

# 4. 午后最大暴跌
show_range("B146", "B158", "机会4: B156 午后反弹至EMA21后最大放量暴跌")

# 5. 下午反弹拒绝
show_range("B165", "B176", "机会5: B169-B170 下午反弹至EMA21后拒绝")

print("\n" + "=" * 90)
print("日总结")
print("=" * 90)
print(f"全天: {df.iloc[0]['open']:.4f} → {df.iloc[-1]['close']:.4f} ({(df.iloc[-1]['close']/df.iloc[0]['open']-1)*100:.2f}%)")
print(f"EMA200: {df.iloc[0]['ema_200']:.4f} → {df.iloc[-1]['ema_200']:.4f}")
print(f"CVD: {df.iloc[0]['cvd']:.0f} → {df.iloc[-1]['cvd']:.0f}")
print(f"bear_stack占比: {(df['ema_stack']=='bear_stack').sum()}/{len(df)}")
print(f"tangled占比: {(df['ema_stack']=='tangled').sum()}/{len(df)}")
print(f"放量暴跌(vol>=3x,delta<-5000): {((df['vol_vs_ma']>=3)&(df['delta']<-5000)).sum()}次")
