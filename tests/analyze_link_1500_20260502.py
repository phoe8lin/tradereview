from pathlib import Path

import pandas as pd

ROOT = Path('/Volumes/external/trade/交易复盘')
path = ROOT / 'reviews/2026-05-02/data/LINK_5m_short_1500_probe.parquet'
df = pd.read_parquet(path)
df['datetime'] = pd.to_datetime(df['datetime'])
df['ema_stack'] = df.apply(
    lambda r: 'bull_stack' if r['ema_21'] > r['ema_55'] > r['ema_100'] > r['ema_200'] else (
        'bear_stack' if r['ema_21'] < r['ema_55'] < r['ema_100'] < r['ema_200'] else 'tangled'
    ),
    axis=1,
)
cols = ['kline_id','datetime','open','high','low','close','volume','ema_21','ema_55','ema_100','ema_200','ema_stack','wave','upper_wick_ratio','lower_wick_ratio','body_ratio','vol_vs_ma']
win = df[(df['datetime'] >= '2026-05-02 14:20') & (df['datetime'] <= '2026-05-02 15:40')][cols].copy()
print(win.to_string(index=False))
anchor = df[df['datetime'] == '2026-05-02 15:00'].iloc[0]
prev = df[df['datetime'] == '2026-05-02 14:55'].iloc[0]
print('\nANCHOR')
for c in cols:
    print(f'{c}: {anchor[c]}')
print('\nENGULF_CHECK')
print(f"prev O/H/L/C: {prev['open']}/{prev['high']}/{prev['low']}/{prev['close']}")
print(f"anchor O/H/L/C: {anchor['open']}/{anchor['high']}/{anchor['low']}/{anchor['close']}")
print(f"bear_body_engulf_prev_body: {anchor['open'] >= prev['close'] and anchor['close'] <= prev['open'] and anchor['close'] < anchor['open']}")
print(f"test_ema100_failed: high={anchor['high']} ema100={anchor['ema_100']} close={anchor['close']} below_ema100={anchor['close'] < anchor['ema_100']}")
entry=9.093
stop=9.109
take=9.068
print('\nTRADE')
print(f'rr={(entry-take)/(stop-entry):.3f}')
after = df[df['datetime'] > '2026-05-02 15:00'].copy().head(20)
for _, r in after.iterrows():
    if r['high'] >= stop or r['low'] <= take:
        print(f"first_hit {r['kline_id']} {r['datetime']} high={r['high']} low={r['low']} outcome={'STOP' if r['high'] >= stop else 'TAKE'}")
        break
print(f"after20 max_high={after['high'].max()} min_low={after['low'].min()}")
