from pathlib import Path

import pandas as pd

ROOT = Path('/Volumes/external/trade/交易复盘')
DAY_PATH = ROOT / 'reviews/2026-05-02/data/day_HYPE_15m.parquet'
SUPP_PATH = ROOT / 'reviews/2026-05-02/data/supp_HYPE_5m.parquet'


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out['time'] = pd.to_datetime(out['datetime']).dt.strftime('%H:%M')
    out['dist_ema21_pct'] = (out['close'] - out['ema_21']) / out['ema_21'] * 100
    out['low_dist_ema21_pct'] = (out['low'] - out['ema_21']) / out['ema_21'] * 100
    out['dist_ema100_pct'] = (out['close'] - out['ema_100']) / out['ema_100'] * 100
    out['cvd_diff'] = out['cvd'].diff().fillna(out['cvd'])
    return out


def print_table(title: str, df: pd.DataFrame, cols: list[str]) -> None:
    print('\n' + title)
    print(df[cols].to_string(index=False, float_format=lambda x: f'{x:.3f}'))


def main() -> None:
    day = enrich(pd.read_parquet(DAY_PATH))
    supp = enrich(pd.read_parquet(SUPP_PATH))

    day_cols = [
        'id', 'time', 'open', 'high', 'low', 'close',
        'ema_21', 'ema_55', 'ema_100', 'ema_stack', 'wave', 'vol_vs_ma',
        'lower_wick_ratio', 'upper_wick_ratio', 'engulf', 'delta', 'cvd',
        'buy_ratio', 'dist_ema21_pct', 'low_dist_ema21_pct', 'dist_ema100_pct',
    ]
    print_table(
        '=== 15m 06:00-17:15 ===',
        day[(day['time'] >= '06:00') & (day['time'] <= '17:15')],
        day_cols,
    )

    supp_cols = [
        'id', 'time', 'open', 'high', 'low', 'close',
        'ema_21', 'ema_55', 'ema_100', 'wave', 'vol_vs_ma',
        'lower_wick_ratio', 'upper_wick_ratio', 'engulf', 'delta', 'cvd',
        'buy_ratio', 'dist_ema21_pct', 'low_dist_ema21_pct',
    ]
    c = supp[(supp['time'] >= '10:30') & (supp['time'] <= '15:00')].copy()
    sel = c[
        (c['low_dist_ema21_pct'] <= 0.05)
        | (c['wave'] <= -35)
        | (c['lower_wick_ratio'] >= 0.5)
        | (c['vol_vs_ma'] >= 1.5)
        | (c['id'].isin(['E87', 'E88', 'E89', 'E100', 'E101', 'E102', 'E103', 'E104', 'E105']))
    ]
    print_table('=== 5m candidate rows 10:30-15:00 ===', sel, supp_cols)

    print('\n=== simple long candidates ===')
    for i in range(1, len(supp) - 1):
        row = supp.iloc[i]
        nxt = supp.iloc[i + 1]
        if not ('12:30' <= row['time'] <= '14:45'):
            continue
        setup = row['wave'] <= -35 or row['low_dist_ema21_pct'] <= -0.25 or row['vol_vs_ma'] >= 1.5
        trigger = nxt['close'] > nxt['open'] and nxt['delta'] > 0 and nxt['close'] >= row['close']
        if setup and trigger:
            print(
                f"setup {row['id']} {row['time']} close={row['close']:.3f} "
                f"wave={row['wave']:.1f} low-ema21%={row['low_dist_ema21_pct']:.3f} "
                f"delta={row['delta']:.0f} cvd={row['cvd']:.0f} -> "
                f"trigger {nxt['id']} {nxt['time']} close={nxt['close']:.3f} "
                f"wave={nxt['wave']:.1f} delta={nxt['delta']:.0f} cvd={nxt['cvd']:.0f}"
            )


if __name__ == '__main__':
    main()
