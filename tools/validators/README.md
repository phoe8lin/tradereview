# validators —— 复盘流程化校验工具集 (P1 + P2 + 形态/regime)

把"凭印象写"的环节固化成函数，杜绝复盘时的低级错误。

## 为什么需要

2026-04-26 复盘出现的错误（已被本工具集捕获）：
- 时区误判（datetime 误加 8h）
- "ema100 被拒" 但价格其实从未触及 ema100
- 入场价 41.215 既不是 OHLC 也不是 swing 高低点
- 止损 41.285 比 ema_200 41.283 还高
- RR 数学不自洽
- 止盈在后续 K 线根本不触及

以上每一条都对应工具中的一项检查，**只要传入参数运行就会被自动报警**。

## 三个核心模块

### `data_loader.py` — 唯一数据入口

```python
from tools.validators import load_day, get_bar, parse_ref

df = load_day("2026-04-26", "HYPE", "5m")    # parquet datetime 已是 UTC+8
bar = get_bar(df, "B193")                     # 也接受 "16:05" / int / Timestamp
```

**铁律**：禁止散落 `pd.read_parquet`，禁止再做时区偏移。

### `price_ema.py` — 价格-EMA 关系结构化

```python
from tools.validators import ema_relation
rel = ema_relation(bar)
# rel['relation']['ema_100']['state'] in {
#   'above_only', 'below_only',           # 整根在均线一侧，未触及
#   'rejected_above', 'rejected_below',   # 触及且 close 收回
#   'touched_neutral'                      # 触及但 close 落在均线上
# }
print(rel['narrative'])
# 例: "B217 18:05 close=41.238: close 在 ema_21/ema_55 之上; close 在 ema_100/ema_200 之下; 下方被托(ema_55)."
```

**严格定义**（不允许再用模糊词）：
- 触及 = `low <= ema <= high`
- 拒绝（做空向）= 触及 + close < ema
- 未触及 = 整根 K 在 ema 单侧

### `trade_params.py` — 交易参数全套验证

```python
from tools.validators import validate_trade
from tools.validators.trade_params import format_report

r = validate_trade(df, "B221", "short",
                    entry=41.215, stop=41.285, take=41.112,
                    claimed_rr=1.47)
print(format_report(r))
```

逐项检查：

| 项 | 含义 |
|---|---|
| A | entry 是否 ∈ OHLC（否则视为限价单）|
| B | 限价单可达性：后续 N 根能否触及 entry |
| C | 止损位置：是否覆盖入场 K 高点；与各 EMA 的相对位置 |
| D | RR 数学一致：(entry-take)/(stop-entry) ≈ claimed_rr |
| E | 后验兑现：take/stop 哪个先触发，多少根之后 |
| F | MAE / MFE：持仓最坏未实现亏损 / 最大未实现盈利 |
| G | 擦边判定：兑现/止损是否 < 5 ticks 极限 |

输出含 `overall ∈ {PASS, WARN, FAIL}`。

### `signal_score.py` — 信号要素打分（P2）

把"5 要素清单"固化成模板。当前支持：
- `short_reversal_overbought`：bull_stack/tangled 中超买回归做空
- `long_reversal_oversold`：bear_stack/tangled 中超卖回归做多

```python
from tools.validators import score_signal
r = score_signal(df, anchor="B122", entry="B123", template="short_reversal_overbought")
# r['score']=5 r['total']=5 r['grade']='A+'
```

5 要素（做空版）：锚 K vol≥1.8 / 入场 K vol≤0.6 / delta<0 / CVD 较锚下降 / close 距 ema_55 ≥ +0.06

阈值集中在 `TEMPLATES[name]['thresholds']`，调参方便。

### `post_verify.py` — 逐根后验明细（P2）

```python
from tools.validators import post_verify
r = post_verify(df, "B221", "short", 41.178, 41.220, 41.123, horizon=20)
# r['timeline']: 每根 K 的 unrealized_R / mae_R / mfe_R / dist_to_stop_R / dist_to_take_R / state
# r['summary']: outcome / fired_at / max_mae_R / max_mfe_R / closest_stop / closest_take
```

所有距离按 R 归一，便于跨标的比较。

### `run_review.py` — 一键 markdown 报告（P2）

输入 `trades_validate.yaml`，输出整份 markdown：

```bash
python -m tools.validators.run_review reviews/2026-04-26/trades_validate.yaml \
    > reviews/2026-04-26/auto_validation.md
```

yaml 格式见 `reviews/2026-04-26/trades_validate.yaml` 样例。

报告含：顶部汇总表（含 overall + signal grade）+ 逐笔详情（锚 K/入场 K 的全部指标 + EMA 关系 + 5 要素表 + 7 项参数校验表）。

### `bar_features.py` — K 线形态字段化判定

杜绝"看起来像长上影""感觉是 absorption"的凭印象判定。

```python
from tools.validators import detect_features, scan_features
r = detect_features(get_bar(df, "B193"))
# r.patterns -> ['long_upper_wick', 'bear_absorption']
# r.evidence -> {'bear_absorption': {'is_bull': False, 'delta': 1766.0, ...}}

scan_features(df, "B190:B225", patterns=["bull_absorption", "bear_absorption"])
# -> 区间内所有 absorption 根
```

支持的形态：`long_upper_wick / long_lower_wick / doji / pin_bar_top / pin_bar_bottom /
bull_absorption / bear_absorption / breakout_bar / engulf_bull / engulf_bear /
high_volume / low_volume`。阈值集中在 `DEFAULT_THRESHOLDS`。

**核心定义（防误读）**：
- `bull_absorption` = 阳线收盘 + delta 显著为负 → 阳吃卖盘
- `bear_absorption` = 阴线收盘 + delta 显著为正 → 阴吃买盘
- 这两个**才是** absorption，B221 那种"阴线 + 大负 delta"是**普通推动**而非 absorption

### `ema_regime.py` — EMA 排列时段切片

杜绝"今天是 bear 趋势日"忽略时段的笼统判定。

```python
from tools.validators import regime_segments, regime_at, regime_summary

regime_segments(df)
# -> [Segment(stack='tangled', start_id='B00', end_id='B26', n_bars=27), ...]

regime_at(df, "B193")
# -> Segment(stack='bear_stack', start_id='B182', start_time='15:10', n_bars=49)

regime_summary(df)
# -> {'dominant_stack': 'tangled', 'dominant_pct': 39.4,
#     'is_trend_day': False, 'n_transitions': 9}
```

2026-04-26 实测：全日切成 10 段、9 次切换，主导是 `tangled` (39.4%) —— **不是趋势日**，
bear_stack 仅在 15:10 之后才形成（B182-B230，49 根）。

## 跑测试

```bash
python tests/validators/test_validators.py        # P1: 9 个
python tests/validators/test_p2.py                # P2: 6 个
python tests/validators/test_features_regime.py   # 形态/regime: 13 个
# 或一次跑全部
python -m unittest discover -s tests/validators
```

合计 **28 个测试**，覆盖 B116/B123/B160（上午三单）、B193/B199/B204/B217/B221/B225（下午相关 K），
同时验证：信号打分、参数校验、yaml→markdown 渲染、形态识别、regime 切片。

## 后续计划（P3）

- 数据 md5 校验：review.md 顶部锁定数据指纹，避免数据更新后老结论失效
- 图文交叉校验：plotly 图与文字报告共用同一份 trades.yaml
- 元统计：跨日期累积 trades，统计"5/5 A+ 的真实胜率"等
- 更多模板：B193 类"bear_stack 中 EMA 拒绝 + absorption"模板
