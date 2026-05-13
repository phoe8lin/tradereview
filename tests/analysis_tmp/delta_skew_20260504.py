from pathlib import Path
import sys

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from tools.validators import load_day, get_bar


def main():
    df = load_day("2026-05-04", "HYPE", "5m")

    # 19:40-20:20 的 delta 分布
    seg1 = df[(df["datetime"].dt.strftime("%H:%M") >= "19:40") & (df["datetime"].dt.strftime("%H:%M") <= "20:20")]
    pos = seg1[seg1["delta"] > 0]["delta"]
    neg = seg1[seg1["delta"] < 0]["delta"]

    print("=== 19:40-20:20 delta 分布 ===")
    print(f"正值: n={len(pos)}, mean={pos.mean():.0f}, max={pos.max():.0f}")
    print(f"负值: n={len(neg)}, mean={neg.mean():.0f}, min={neg.min():.0f}")
    print(f"CVD: {seg1['cvd'].iloc[0]:.0f} → {seg1['cvd'].iloc[-1]:.0f} (Δ{seg1['cvd'].iloc[-1] - seg1['cvd'].iloc[0]:.0f})")
    print(f"偏态: 正均值{pos.mean():.0f} vs 负均值{neg.mean():.0f}")

    # 20:20-20:50 的 delta 分布
    seg2 = df[(df["datetime"].dt.strftime("%H:%M") >= "20:20") & (df["datetime"].dt.strftime("%H:%M") <= "20:50")]
    pos2 = seg2[seg2["delta"] > 0]["delta"]
    neg2 = seg2[seg2["delta"] < 0]["delta"]

    print("\n=== 20:20-20:50 delta 分布 ===")
    print(f"正值: n={len(pos2)}, mean={pos2.mean():.0f}, max={pos2.max():.0f}")
    print(f"负值: n={len(neg2)}, mean={neg2.mean():.0f}, min={neg2.min():.0f}")
    print(f"CVD: {seg2['cvd'].iloc[0]:.0f} → {seg2['cvd'].iloc[-1]:.0f} (Δ{seg2['cvd'].iloc[-1] - seg2['cvd'].iloc[0]:.0f})")
    print(f"偏态: 正均值{pos2.mean():.0f} vs 负均值{neg2.mean():.0f}")

    # 19:40-20:50 完整段
    seg3 = df[(df["datetime"].dt.strftime("%H:%M") >= "19:40") & (df["datetime"].dt.strftime("%H:%M") <= "20:50")]
    pos3 = seg3[seg3["delta"] > 0]["delta"]
    neg3 = seg3[seg3["delta"] < 0]["delta"]

    print("\n=== 19:40-20:50 完整段 delta 分布 ===")
    print(f"正值: n={len(pos3)}, mean={pos3.mean():.0f}, max={pos3.max():.0f}")
    print(f"负值: n={len(neg3)}, mean={neg3.mean():.0f}, min={neg3.min():.0f}")
    print(f"CVD: {seg3['cvd'].iloc[0]:.0f} → {seg3['cvd'].iloc[-1]:.0f} (Δ{seg3['cvd'].iloc[-1] - seg3['cvd'].iloc[0]:.0f})")

    # EMA 位置
    b244 = get_bar(df, "20:20")
    b245 = get_bar(df, "20:25")
    b247 = get_bar(df, "20:35")
    b252 = get_bar(df, "21:00")
    b254 = get_bar(df, "21:10")

    print("\n=== EMA 位置 ===")
    for b, label in [(b244, "B244 20:20"), (b245, "B245 20:25"), (b247, "B247 20:35"), (b252, "B252 21:00"), (b254, "B254 21:10")]:
        c = float(b["close"])
        e21 = float(b["ema_21"])
        e55 = float(b["ema_55"])
        print(f"{label}: close={c:.3f}, ema21={e21:.3f}, ema55={e55:.3f}, "
              f"vs_ema21={'上' if c > e21 else '下'}, vs_ema55={'上' if c > e55 else '下'}, "
              f"stack={b['ema_stack']}")


if __name__ == "__main__":
    main()
