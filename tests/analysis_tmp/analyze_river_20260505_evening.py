"""分析 RIVER 5m 2026-05-05 22:00 之后做空机会"""
import pandas as pd
from pathlib import Path

pq = Path("/Volumes/external/trade/交易复盘/reviews/2026-05-05/data/supp_RIVER_5m.parquet")
df = pd.read_parquet(pq).set_index("id")

cols = ["datetime","open","high","low","close","vol_vs_ma","ema_21","ema_55","ema_200",
        "ema_stack","wave","body_ratio","upper_wick_ratio","lower_wick_ratio",
        "engulf","delta","cvd","buy_ratio"]
print(df[cols].to_string())

print("\n=== EMA轨迹 ===")
for idx in df.index:
    r = df.loc[idx]
    t = str(r['datetime'])[-8:-3]
    print(f"{idx} {t} E21={r['ema_21']:.4f} E55={r['ema_55']:.4f} E200={r['ema_200']:.4f} {r['ema_stack']}")

print("\n=== 候选做空入场点 ===")
# 识别回踩均线+拒绝的K线
candidates = []
for i, idx in enumerate(df.index):
    r = df.loc[idx]
    score = 0
    reasons = []
    
    # 条件1: bear_stack
    if r["ema_stack"] == "bear_stack":
        score += 2
        reasons.append("bear_stack")
    
    # 条件2: 长上影 (测试上方阻力)
    if r["upper_wick_ratio"] >= 0.4:
        score += 2
        reasons.append(f"长上影={r['upper_wick_ratio']:.3f}")
    
    # 条件3: bear engulf
    if r["engulf"] == "bear":
        score += 2
        reasons.append("bear_engulf")
    
    # 条件4: delta为负
    if r["delta"] < -1000:
        score += 1
        reasons.append(f"delta={r['delta']:.0f}")
    
    # 条件5: 放量
    if r["vol_vs_ma"] >= 1.5:
        score += 1
        reasons.append(f"放量={r['vol_vs_ma']:.2f}x")
    
    # 条件6: Wave超买区
    if r["wave"] > 30:
        score += 1
        reasons.append(f"Wave={r['wave']:.1f}")
    
    # 条件7: 强实体阴线
    if r["body_ratio"] >= 0.5 and r["close"] < r["open"]:
        score += 1
        reasons.append(f"强阴线body={r['body_ratio']:.3f}")
    
    # 条件8: 价格接近EMA21 (回踩)
    price_near_ema21 = abs(r["high"] - r["ema_21"]) / r["ema_21"] < 0.005
    if price_near_ema21:
        score += 1
        reasons.append("近EMA21")
    
    if score >= 3:
        candidates.append((idx, score, reasons, r))

print(f"\n找到 {len(candidates)} 个候选入场点:\n")
for idx, score, reasons, r in candidates:
    t = str(r['datetime'])[-8:-3]
    print(f"  {idx} {t} | O={r['open']:.4f} H={r['high']:.4f} L={r['low']:.4f} C={r['close']:.4f}")
    print(f"    评分={score} | {' | '.join(reasons)}")
    print(f"    EMA21={r['ema_21']:.4f} EMA55={r['ema_55']:.4f} EMA200={r['ema_200']:.4f}")
    print(f"    delta={r['delta']:.0f} buy_ratio={r['buy_ratio']:.3f} CVD={r['cvd']:.0f}")
    print()

# 分析回踩均线序列
print("\n=== 回踩均线序列识别 ===")
# 找反弹到EMA附近然后被拒绝的序列
for i in range(2, len(df)):
    prev2 = df.iloc[i-2]
    prev1 = df.iloc[i-1]
    curr = df.iloc[i]
    idx = df.index[i]
    
    # 反弹: prev2或prev1的high接近EMA21
    bounce_high = max(prev2["high"], prev1["high"])
    near_ema = abs(bounce_high - curr["ema_21"]) / curr["ema_21"] < 0.005
    
    # 当前K线拒绝: 阴线 + upper_wick
    is_reject = (curr["close"] < curr["open"] and curr["upper_wick_ratio"] >= 0.3)
    
    if near_ema and is_reject and curr["ema_stack"] == "bear_stack":
        t = str(curr['datetime'])[-8:-3]
        print(f"  {idx} {t}: 反弹至{bounce_high:.4f}(近EMA21={curr['ema_21']:.4f})→拒绝 C={curr['close']:.4f}")
        print(f"    upper_wick={curr['upper_wick_ratio']:.3f} delta={curr['delta']:.0f} vol={curr['vol_vs_ma']:.2f}x")
