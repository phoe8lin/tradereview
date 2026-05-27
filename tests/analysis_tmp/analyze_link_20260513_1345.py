from pathlib import Path
import sys
import pandas as pd

ROOT = Path('/Volumes/external/trade/交易复盘')
sys.path.insert(0, str(ROOT))

from tools.volume_profile import build_from_ohlcv

day = pd.read_parquet(ROOT / 'reviews/2026-05-13/data/day_LINK_15m.parquet')
supp = pd.read_parquet(ROOT / 'reviews/2026-05-13/data/supp_LINK_5m.parquet')

for df in (day, supp):
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['time'] = df['datetime'].dt.strftime('%H:%M')

cols = ['id','time','open','high','low','close','volume','vol_vs_ma','ema_21','ema_55','ema_100','ema_200','ema_stack','wave','body_ratio','upper_wick_ratio','lower_wick_ratio','engulf','delta','cvd','buy_ratio']
print('--- 15m 12:45-14:45 ---')
print(day[(day['time'] >= '12:45') & (day['time'] <= '14:45')][cols].to_string(index=False, float_format=lambda x: f'{x:.3f}'))
print('\n--- 5m 13:00-14:30 ---')
print(supp[cols].to_string(index=False, float_format=lambda x: f'{x:.3f}'))

# VP windows using OHLCV approximation because day_review_builder did not persist raw trades.
# 1) 15m structure window: 12:45-14:30
vp15_win = day[(day['time'] >= '12:45') & (day['time'] <= '14:30')]
vp15 = build_from_ohlcv(vp15_win, n_bins=40)
# 2) 5m pullback/trigger window: 13:00-14:30
vp5 = build_from_ohlcv(supp, n_bins=40)
# 3) pre-trigger distribution 13:00-13:40
vp5_pre = build_from_ohlcv(supp[supp['time'] <= '13:40'], n_bins=32)
# 4) trigger+follow-through 13:45-14:30
vp5_post = build_from_ohlcv(supp[supp['time'] >= '13:45'], n_bins=32)

print('\n--- VP OHLCV approximation ---')
for name, vp in [('15m_1245_1430', vp15), ('5m_1300_1430', vp5), ('5m_pre_1300_1340', vp5_pre), ('5m_post_1345_1430', vp5_post)]:
    print(f'{name}: POC={vp.poc:.4f}, VAL={vp.va_low:.4f}, VAH={vp.va_high:.4f}, total_vol={vp.total_vol:.1f}, HVN={[round(x,4) for x in vp.hvn[:5]]}, LVN={[round(x,4) for x in vp.lvn[:5]]}')

# Local metrics
b55 = day.loc[day['time'] == '13:45'].iloc[0]
e09 = supp.loc[supp['time'] == '13:45'].iloc[0]
pre = supp[supp['time'] < '13:45']
post = supp[supp['time'] >= '13:45']
print('\n--- Derived metrics ---')
print(f"B54-B58 delta_sum={day[(day['time']>='13:30') & (day['time']<='14:30')]['delta'].sum():.3f}")
print(f"E03-E08 selloff delta_sum={supp[(supp['time']>='13:15') & (supp['time']<='13:40')]['delta'].sum():.3f}, cvd_change={supp.loc[supp['time']=='13:40','cvd'].iloc[0] - supp.loc[supp['time']=='13:15','cvd'].iloc[0]:.3f}")
print(f"E09-E17 rebound delta_sum={post['delta'].sum():.3f}, cvd_change={post['cvd'].iloc[-1] - post['cvd'].iloc[0]:.3f}")
print(f"E09 close_vs_vp5_poc={e09['close'] - vp5.poc:.4f}, close_vs_vp5_pre_poc={e09['close'] - vp5_pre.poc:.4f}, low_vs_ema200={e09['low'] - e09['ema_200']:.4f}, close_vs_ema21={e09['close'] - e09['ema_21']:.4f}")
print(f"B55 close_vs_ema200={b55['close'] - b55['ema_200']:.4f}, close_vs_ema21={b55['close'] - b55['ema_21']:.4f}, low_vs_ema200={b55['low'] - b55['ema_200']:.4f}")
