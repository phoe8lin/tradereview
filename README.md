# 交易复盘工具集

以 TradingView 截图为起点，用 ccxt 复刻底层数据 + 指标 + 交互图表，累积结构化复盘记录。

## 目录结构

```
交易复盘/
├── tools/                # 工具集（数据/指标/编号/绘图/打包）
├── config/defaults.yaml  # 业务参数（指标周期、阈值、颜色等）
├── reviews/YYYY-MM-DD/   # 每日复盘包
│   ├── raw/              # TV 原截图
│   ├── data/             # OHLCV + 指标快照（parquet）
│   ├── replicated/       # 复刻的交互式图表（Plotly HTML）
│   ├── trades/           # 每笔交易 yaml 结构化元数据
│   └── review.md         # 当日复盘对话沉淀
└── index/trades.sqlite   # 跨复盘检索索引
```

## 约定

- 时区：全局 **UTC+8**（截图、编号、展示、存储统一）。
- 默认交易所：**Binance USDT-M 永续**。
- K 线编号：按可见窗口从左到右 `A001, A002, ...`，hover 显示 UTC+8 时间 + OHLCV。
- 指标：
  - EMA **21 / 55 / 100 / 200**
  - Wave Filter（StochRSI 变种，与 `pinescript指标/超买超卖过滤器.pine` 一致）
- 预热：默认拉 600 根，展示窗口约 200 根，保证 EMA200 和 Wave Filter 稳定。

## 最小调用示例

```python
from tools.review_builder import build_review

build_review(
    review_date="2026-04-19",
    trade_id="BTC_1h_001",
    symbol_base="BTC",
    symbol_quote="USDT",
    timeframe="1h",
    anchor_time="2026-04-18 14:00",  # 用户截图中的"分析中心 K 线"时间 (UTC+8)
    entry=62340.0,
    stop=61800.0,
    take=64000.0,
    direction="long",
    raw_screenshot="reviews/2026-04-19/raw/btc_1h_001.png",
    notes="均线多头排列回踩 EMA55，Wave Filter 在超卖上穿",
)
```

执行后生成：
- `reviews/2026-04-19/data/BTC_1h_001.parquet`（OHLCV + 指标 + 特征）
- `reviews/2026-04-19/replicated/BTC_1h_001.html`（交互式图表）
- `reviews/2026-04-19/trades/BTC_1h_001.yaml`（结构化元数据）

## 运行

```bash
/opt/anaconda3/envs/trade/bin/python -m tools.review_builder --help
```
