from pathlib import Path

import pandas as pd

ROOT = Path('/Volumes/external/trade/交易复盘')
path = ROOT / 'reviews/2026-05-18/data/day_AAVE_5m.parquet'
df = pd.read_parquet(path).copy()
df['datetime'] = pd.to_datetime(df['datetime'])
win = df[(df['datetime'] >= pd.Timestamp('2026-05-18 19:15')) & (df['datetime'] <= pd.Timestamp('2026-05-18 23:30'))].copy().reset_index(drop=True)

# add local window numbering while keeping original B id
win['W'] = [f'W{i:02d}' for i in range(len(win))]

prev_low = win['low'].shift(1)
prev_close = win['close'].shift(1)
win['ema200_relation'] = win.apply(lambda r: 'below' if r.close < r.ema_200 else ('above' if r.close > r.ema_200 else 'at'), axis=1)
win['reject_ema21'] = (win['high'] >= win['ema_21']) & (win['close'] < win['ema_21'])
win['reject_ema55'] = (win['high'] >= win['ema_55']) & (win['close'] < win['ema_55'])
win['reject_ema100'] = (win['high'] >= win['ema_100']) & (win['close'] < win['ema_100'])
win['reject_ema200'] = (win['high'] >= win['ema_200']) & (win['close'] < win['ema_200'])
win['break_prev_low'] = win['close'] < prev_low
win['bear_bar'] = win['close'] < win['open']
win['wave_turn_down'] = win['wave'] < win['wave'].shift(1)
win['vol_spike'] = win['vol_vs_ma'] >= 1.5

score = []
reasons = []
for i, r in win.iterrows():
    s = 0
    rs = []
    if r['ema_stack'] == 'bear_stack':
        s += 2; rs.append('EMA空排')
    elif r['ema_stack'] == 'tangled' and r['close'] < r['ema_200']:
        s += 1; rs.append('缠绕但收盘在EMA200下')
    if r['wave'] >= 35:
        s += 2; rs.append('Wave高位')
    elif r['wave_turn_down'] and win['wave'].shift(1).iloc[i] >= 35:
        s += 2; rs.append('Wave高位回落')
    if r['engulf'] == 'bear':
        s += 2; rs.append('bear engulf')
    if r['upper_wick_ratio'] >= 0.4:
        s += 1; rs.append('长上影')
    if r['body_ratio'] >= 0.55 and r['bear_bar']:
        s += 1; rs.append('阴线实体')
    if r['vol_spike']:
        s += 1; rs.append('放量')
    if r['reject_ema21'] or r['reject_ema55'] or r['reject_ema100'] or r['reject_ema200']:
        s += 1; rs.append('EMA拒绝')
    if r['break_prev_low']:
        s += 1; rs.append('跌破前低')
    score.append(s)
    reasons.append('+'.join(rs))
win['short_score'] = score
win['reasons'] = reasons

cols = ['W','id','datetime','open','high','low','close','volume','vol_vs_ma','ema_21','ema_55','ema_100','ema_200','ema_stack','wave','body_ratio','upper_wick_ratio','lower_wick_ratio','engulf','short_score','reasons']
print('WINDOW SUMMARY')
print(win[['W','id','datetime','open','high','low','close','ema_stack','wave','vol_vs_ma','engulf','upper_wick_ratio','body_ratio']].to_string(index=False, float_format=lambda x: f'{x:.3f}'))
print('\nCANDIDATES score>=5')
cand = win[win['short_score'] >= 5].copy()
print(cand[cols].to_string(index=False, float_format=lambda x: f'{x:.3f}'))
print('\nSTRUCTURE')
print('n=', len(win), 'high=', win.loc[win.high.idxmax(), ['W','id','datetime','high']].to_dict(), 'low=', win.loc[win.low.idxmin(), ['W','id','datetime','low']].to_dict())
print('ema_stack_counts=', win['ema_stack'].value_counts().to_dict())
print('wave max=', win.loc[win.wave.idxmax(), ['W','id','datetime','wave','high','close']].to_dict())
print('wave min=', win.loc[win.wave.idxmin(), ['W','id','datetime','wave','low','close']].to_dict())
