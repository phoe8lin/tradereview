from pathlib import Path
import time

import pandas as pd
import requests

ROOT = Path('/Volumes/external/trade/交易复盘')
DATA = ROOT / 'reviews/2026-05-18/data/day_AAVE_5m.parquet'
OUT = ROOT / 'reviews/2026-05-18/data/aave_20260518_1915_2330.orderflow.parquet'
TRADES = ROOT / 'reviews/2026-05-18/data/aave_20260518_1915_2330.trades.parquet'
PROXY_OPTIONS = [
    {'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'},
    None,
]
TF_MS = 300_000

klines = pd.read_parquet(DATA).copy()
klines['datetime'] = pd.to_datetime(klines['datetime'])
win = klines[(klines['datetime'] >= pd.Timestamp('2026-05-18 19:15')) & (klines['datetime'] <= pd.Timestamp('2026-05-18 23:30'))].copy().reset_index(drop=True)
start_ms = int(win['timestamp'].iloc[0])
end_ms = int(win['timestamp'].iloc[-1]) + TF_MS

rows = []
cursor = start_ms
session = requests.Session()
while cursor < end_ms:
    params = {'symbol': 'AAVEUSDT', 'startTime': cursor, 'endTime': end_ms - 1, 'limit': 1000}
    last_error = None
    resp = None
    for attempt in range(6):
        proxies = PROXY_OPTIONS[attempt % len(PROXY_OPTIONS)]
        try:
            resp = session.get('https://fapi.binance.com/fapi/v1/aggTrades', params=params, proxies=proxies, timeout=20)
            resp.raise_for_status()
            break
        except Exception as exc:
            last_error = exc
            print(f'[warn] request failed attempt={attempt + 1} proxies={bool(proxies)} error={exc}')
            time.sleep(2.0)
    if resp is None:
        raise RuntimeError(f'request failed: {last_error}')
    batch = resp.json()
    if not batch:
        break
    rows.extend(batch)
    last_ts = int(batch[-1]['T'])
    if last_ts <= cursor:
        break
    cursor = last_ts + 1
    if len(batch) < 1000:
        break
    time.sleep(0.05)

trades = pd.DataFrame(rows)
if trades.empty:
    raise RuntimeError('no trades fetched')
trades = trades.rename(columns={'T': 'timestamp', 'p': 'price', 'q': 'amount', 'm': 'buyer_is_maker'})
trades = trades[['timestamp', 'price', 'amount', 'buyer_is_maker']].copy()
trades['timestamp'] = trades['timestamp'].astype('int64')
trades['price'] = trades['price'].astype(float)
trades['amount'] = trades['amount'].astype(float)
trades['side'] = trades['buyer_is_maker'].map(lambda x: 'sell' if bool(x) else 'buy')
trades = trades[(trades['timestamp'] >= start_ms) & (trades['timestamp'] < end_ms)].drop_duplicates().sort_values('timestamp').reset_index(drop=True)
trades['bar_ts'] = (trades['timestamp'] // TF_MS) * TF_MS
trades['buy_vol'] = trades.apply(lambda r: r['amount'] if r['side'] == 'buy' else 0.0, axis=1)
trades['sell_vol'] = trades.apply(lambda r: r['amount'] if r['side'] == 'sell' else 0.0, axis=1)

grp = trades.groupby('bar_ts').agg(buy_vol=('buy_vol', 'sum'), sell_vol=('sell_vol', 'sum')).reset_index().rename(columns={'bar_ts': 'timestamp'})
grp['total_trades_vol'] = grp['buy_vol'] + grp['sell_vol']
grp['delta'] = grp['buy_vol'] - grp['sell_vol']
of = win[['id', 'datetime', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'vol_vs_ma', 'ema_stack', 'wave', 'body_ratio', 'upper_wick_ratio', 'engulf']].merge(grp, on='timestamp', how='left')
of[['buy_vol', 'sell_vol', 'total_trades_vol', 'delta']] = of[['buy_vol', 'sell_vol', 'total_trades_vol', 'delta']].fillna(0.0)
of['cvd'] = of['delta'].cumsum()
of['buy_ratio'] = of.apply(lambda r: r['buy_vol'] / r['total_trades_vol'] if r['total_trades_vol'] > 0 else float('nan'), axis=1)
trades.to_parquet(TRADES, index=False)
of.to_parquet(OUT, index=False)
print({'trades': len(trades), 'bars': len(of), 'out': str(OUT.relative_to(ROOT))})
key_ids = ['B237', 'B249', 'B257', 'B263', 'B276', 'B279', 'B281', 'B282']
cols = ['id', 'datetime', 'open', 'high', 'low', 'close', 'vol_vs_ma', 'ema_stack', 'wave', 'engulf', 'buy_vol', 'sell_vol', 'delta', 'cvd', 'buy_ratio']
print(of[of['id'].isin(key_ids)][cols].to_string(index=False, float_format=lambda x: f'{x:.3f}'))
print('\nTOP NEGATIVE DELTA')
print(of.nsmallest(10, 'delta')[cols].to_string(index=False, float_format=lambda x: f'{x:.3f}'))
print('\nTOP POSITIVE DELTA')
print(of.nlargest(10, 'delta')[cols].to_string(index=False, float_format=lambda x: f'{x:.3f}'))
