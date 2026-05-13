from dataclasses import asdict, dataclass
from pathlib import Path
import sys

import pandas as pd
import yaml

ROOT = Path('/Volumes/external/trade/交易复盘')
sys.path.insert(0, str(ROOT))

from tools.chart_replicator import build_chart

DATE = '2026-05-02'
DATA_PATH = ROOT / 'reviews' / DATE / 'data' / 'supp_HYPE_5m.parquet'
REPLICATED_DIR = ROOT / 'reviews' / DATE / 'replicated'
TRADES_DIR = ROOT / 'reviews' / DATE / 'trades'


@dataclass
class Trade:
    trade_id: str
    anchor_id: str
    anchor_time: str
    direction: str
    entry: float
    stop: float
    take: float
    rr: float
    notes: str
    data_path: str
    chart_path: str


def prepare_chart_df(df: pd.DataFrame, anchor_id: str) -> pd.DataFrame:
    out = df.copy()
    out['kline_id'] = out['id']
    out['is_anchor'] = out['id'] == anchor_id
    out['in_display'] = True
    out['datetime'] = pd.to_datetime(out['datetime'])
    return out


def make_trade(df: pd.DataFrame, trade_id: str, anchor_id: str, entry: float, stop: float, take: float, notes: str) -> Trade:
    rr = round((take - entry) / (entry - stop), 3)
    chart_path = REPLICATED_DIR / f'{trade_id}.html'
    chart_df = prepare_chart_df(df, anchor_id)
    title = f'BINANCEUSDM HYPE/USDT PERP · 5m · {trade_id} · anchor {anchor_id}'
    build_chart(
        chart_df,
        title=title,
        entry=entry,
        stop=stop,
        take=take,
        direction='long',
        output_html=str(chart_path),
        label_every=6,
    )
    row = df.loc[df['id'] == anchor_id].iloc[0]
    trade = Trade(
        trade_id=trade_id,
        anchor_id=anchor_id,
        anchor_time=pd.to_datetime(row['datetime']).strftime('%Y-%m-%d %H:%M'),
        direction='long',
        entry=entry,
        stop=stop,
        take=take,
        rr=rr,
        notes=notes,
        data_path=str(DATA_PATH.relative_to(ROOT)),
        chart_path=str(chart_path.relative_to(ROOT)),
    )
    yaml_path = TRADES_DIR / f'{trade_id}.yaml'
    with open(yaml_path, 'w', encoding='utf-8') as f:
        yaml.safe_dump(asdict(trade), f, allow_unicode=True, sort_keys=False)
    return trade


def main() -> None:
    REPLICATED_DIR.mkdir(parents=True, exist_ok=True)
    TRADES_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_parquet(DATA_PATH)

    trades = [
        make_trade(
            df,
            trade_id='HYPE_5m_long_1320',
            anchor_id='E88',
            entry=41.273,
            stop=41.158,
            take=41.499,
            notes='13:20 wave超卖后delta转正；止损为E87结构低点41.179下方约0.05%缓冲；止盈取前高/整理上沿41.499。',
        ),
        make_trade(
            df,
            trade_id='HYPE_5m_long_1425',
            anchor_id='E101',
            entry=41.246,
            stop=41.169,
            take=41.720,
            notes='14:25 E100放量下探后E101主动买盘反包；止损为E100结构低点41.190下方约0.05%缓冲；止盈取后续第一段放量加速高点41.720。',
        ),
    ]

    for trade in trades:
        print(f'{trade.trade_id} anchor={trade.anchor_id} entry={trade.entry} stop={trade.stop} take={trade.take} rr={trade.rr} chart={trade.chart_path}')


if __name__ == '__main__':
    main()
