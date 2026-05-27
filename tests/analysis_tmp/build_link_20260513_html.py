from pathlib import Path
import sys

import pandas as pd

ROOT = Path('/Volumes/external/trade/交易复盘')
sys.path.insert(0, str(ROOT))

from tools.chart_replicator import build_chart
from tools.volume_profile import build_from_ohlcv, build_micro_vps

OUT_DIR = ROOT / 'reviews/2026-05-13/replicated'
OUT_DIR.mkdir(parents=True, exist_ok=True)


def prepare(df: pd.DataFrame, anchor_id: str) -> pd.DataFrame:
    out = df.copy()
    out['datetime'] = pd.to_datetime(out['datetime'])
    out['kline_id'] = out['id']
    out['is_anchor'] = out['id'] == anchor_id
    out['in_display'] = True
    return out


def render(path: Path, df: pd.DataFrame, anchor_id: str, title: str, entry=None, stop=None, take=None):
    chart_df = prepare(df, anchor_id)
    orderflow = chart_df[['timestamp', 'delta', 'cvd']].copy()
    vp = build_from_ohlcv(chart_df, n_bins=60)
    anchor_idx = int(chart_df.index[chart_df['is_anchor']][0])
    lo_i = max(0, anchor_idx - 5)
    hi_i = min(len(chart_df) - 1, anchor_idx + 8)
    micro = build_micro_vps(chart_df.iloc[lo_i:hi_i + 1].reset_index(drop=True), n_bins=12)
    build_chart(
        chart_df,
        title=title,
        entry=entry,
        stop=stop,
        take=take,
        direction='long',
        output_html=str(path),
        label_every=3,
        orderflow=orderflow,
        big_vp=vp,
        micro_vps=micro,
    )
    print(path.relative_to(ROOT))


day = pd.read_parquet(ROOT / 'reviews/2026-05-13/data/day_LINK_15m.parquet')
supp = pd.read_parquet(ROOT / 'reviews/2026-05-13/data/supp_LINK_5m.parquet')

render(
    OUT_DIR / 'LINK_15m_1345_pullback_long.html',
    day[(pd.to_datetime(day['datetime']).dt.strftime('%H:%M') >= '12:00') & (pd.to_datetime(day['datetime']).dt.strftime('%H:%M') <= '15:30')].reset_index(drop=True),
    'B55',
    'LINK/USDT PERP · 15m · 13:45 EMA200 pullback long review',
    entry=10.420,
    stop=10.391,
    take=10.500,
)

render(
    OUT_DIR / 'LINK_5m_1345_pullback_long.html',
    supp,
    'E09',
    'LINK/USDT PERP · 5m · 13:45 trigger + orderflow/VP review',
    entry=10.413,
    stop=10.391,
    take=10.474,
)
