from pathlib import Path

import pandas as pd

ROOT = Path('/Volumes/external/trade/交易复盘')
klines = pd.read_parquet(ROOT / 'reviews/2026-05-02/data/LINK_5m_short_1500_probe.parquet')
of = pd.read_parquet(ROOT / 'reviews/2026-05-02/data/LINK_5m_short_1500_probe.orderflow.parquet')
klines['datetime'] = pd.to_datetime(klines['datetime'])
of['datetime'] = pd.to_datetime(of['datetime'])
cols = ['kline_id','datetime','close','buy_vol','sell_vol','total_trades_vol','delta','cvd','buy_ratio']
print(of[cols].to_string(index=False))
print('\nMERGED_ANCHOR')
a = of[of['datetime'] == '2026-05-02 15:00'].iloc[0]
for c in cols:
    print(f'{c}: {a[c]}')
prev = of[of['datetime'] < '2026-05-02 15:00'].tail(4)
post = of[of['datetime'] > '2026-05-02 15:00'].head(4)
print('\nSUMMARY')
print(f"prev4_delta_sum={prev['delta'].sum():.3f} prev4_cvd_from={prev['cvd'].iloc[0]:.3f}_to={prev['cvd'].iloc[-1]:.3f}")
print(f"post4_delta_sum={post['delta'].sum():.3f} post4_cvd_from={post['cvd'].iloc[0]:.3f}_to={post['cvd'].iloc[-1]:.3f}")
print(f"anchor_buy_ratio={a['buy_ratio']:.3f} anchor_delta={a['delta']:.3f}")
