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
- K 线编号：**锚 K（用户截图中的位置1）= `A0`，之前为 `A-1, A-2, ...`，之后为 `A1, A2, ...`**；hover 显示 UTC+8 时间 + OHLCV + 指标。
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

## 订单流（按需启用）

`review_builder` 默认只拉 OHLCV。当需要分析 **Delta / CVD / buy_ratio** 来识别"吸收"、"被吸收的假突破"、"CVD-价格背离"等 OHLCV 看不到的信号时，手动调用 `orderflow_fetcher`：

```bash
# 在已生成的复盘包上追加订单流（锚 K 前后各 10 根，共 21 根）
/opt/anaconda3/envs/trade/bin/python -m tools.orderflow_fetcher \
    --date 2026-04-20 --trade-id HYPE_5m_001 --window 10

# 查看订单流表格
/opt/anaconda3/envs/trade/bin/python -m tools.calibration.orderflow_check \
    --date 2026-04-20 --trade-id HYPE_5m_001 --window 10
```

产物：
- `<trade_id>.trades.parquet` — 原始 aggTrades 缓存（二次分析复用）
- `<trade_id>.orderflow.parquet` — 每根 K 线的 `buy_vol / sell_vol / delta / cvd / buy_ratio`

**已存在 trades 缓存则跳过重拉**。可用 `--force-refetch` 强制重拉。

---

## 前端演进路线

### 方案 A（当前）：Python + Plotly.js

- Python 后端 `chart_replicator.py` 直接生成包含 Plotly.js CDN 的 HTML 文件
- 优点：零前端工程，单文件 HTML 离线可看
- 缺点：视觉中规中矩，K 线 500+ 根开始掉帧，定制空间有限

### 方案 B（待切换）：Python 产数据 + TypeScript + TradingView Lightweight Charts

**等积累 5-10 笔复盘、发现 Plotly 明显不够用时再切换**。目标视觉追齐 TradingView 本尊。

#### 架构

```
Python 后端（保留）               前端（新增）
────────────────────           ───────────────
review_builder.py              web/
 ├─ 拉数据/算指标（不变）       ├─ index.html     统一入口
 └─ 产出 <trade_id>.json  ───> ├─ src/
    替代当前的 HTML 生成       │   ├─ main.ts       入口（读 ?trade= 参数）
                               │   ├─ chart.ts      Lightweight Charts 封装
                               │   ├─ overlays.ts   锚K高亮/RR区/K线编号
                               │   └─ hover.ts      自定义悬浮层
                               ├─ vite.config.ts
                               ├─ package.json
                               └─ tsconfig.json
```

JSON 数据约定（`reviews/<date>/trades/<trade_id>.json`）：

```json
{
  "meta": { "symbol": "HYPE/USDT", "timeframe": "5m", "anchor_cn": "2026-04-17 14:40",
            "entry": 43.765, "stop": 43.904, "take": 43.584, "direction": "short" },
  "bars": [ { "time": "2026-04-17 14:40", "o":43.843, "h":43.860, "l":43.750, "c":43.768,
              "v":14163.75, "id":"A0", "isAnchor":true,
              "ema21":43.570, "ema55":43.533, "ema100":43.600, "ema200":43.818,
              "wave":46.86, "bodyRatio":0.68, "upperWick":0.15, "lowerWick":0.16,
              "volVsMa":1.43, "engulf":"bear" } ]
}
```

#### 选用 Lightweight Charts 的理由

- **TradingView 官方开源（MIT）**：K 线样式、十字光标、坐标轴风格与 TV 一致，视觉几乎无割裂
- **Canvas 渲染**：万根 K 线滚动缩放流畅（Plotly 的 SVG 在 500+ 根就开始掉帧）
- **体积小**：压缩后 ~45KB vs Plotly ~3MB
- **多 pane 原生支持**：Wave Filter 子图可作为独立 pane 直接叠加
- **API 简洁**：`createPriceLine` 画入场/止损/止盈线、`setMarkers` 画 K 线编号、`addLineSeries` 画 EMA 组都有一等公民 API

#### 方案 B 的新增工作量（预估 1-2 天）

1. 在 `web/` 下搭 Vite + TS + pnpm 工程骨架
2. `main.ts` 解析 URL 参数加载对应 JSON
3. `chart.ts` 封装 K 线 + EMA 组 + Wave pane + RR 区 + 锚 K 高亮 + K 线编号 markers + 自定义 hover
4. 修改 `review_builder.py`：保留 Plotly HTML 输出不变，**额外**写一份 `<trade_id>.json`
5. 引入 `pnpm build` 步骤，产物放入 `web/dist/` 以便本地静态服务
6. 更新 `/复盘` 工作流文档，增加打开 `web/index.html?trade=<id>` 的步骤

#### 迁移策略（非破坏性）

- Plotly HTML **不删**，作为"快照图表"保留在 `reviews/<date>/replicated/`
- Lightweight Charts 图表由 `web/index.html?trade=<id>` 访问，数据从 `reviews/<date>/trades/<id>.json` 读
- 两者**共存一段时间**，确认新版稳定后再决定是否停掉 Plotly 输出

#### 触发切换的信号

当出现以下任一情况时启动方案 B：
- 某笔复盘需要 500+ 根 K 线，Plotly 明显卡顿
- 需要在同一图上叠加多笔交易（跨日/跨策略对比）
- 想做"拖拽 entry/stop/take 实时重算 RR"这类交互
- 单纯审美疲劳，想要更像 TV 的感觉

