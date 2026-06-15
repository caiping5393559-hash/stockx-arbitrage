# StockX / GOAT 套利扫描器 MVP

本项目是一个本地可运行的 Streamlit + SQLite MVP，用于导入 SKU 清单、抓取 StockX 接口数据、保存原始 JSON、计算 Ask Depth 买断机会、查看单尺码详情，并记录持仓。

## 功能

- 支持 `.csv` / `.xlsx` SKU 清单导入，Excel 支持多 sheet
- 自动解析 SKU / styleNo，并保存原始表格行、排名字段
- StockX 接口客户端支持 query/header/both 三种 token/auth 传递方式
- 所有接口调用 timeout 默认为 20 秒，异常写入 `sync_logs`
- 所有接口原始 JSON 写入 `raw_api_responses`
- 保存商品、市场快照、成交历史、Ask Depth、Bid Depth
- 按 SKU + 尺码计算 100 分套利评分和 S/A/B+/B/C/D 评级
- 提供今日机会、单尺码详情、持仓管理、日志/原始 JSON 页面
- 前端展示统一使用 US 尺码和 USD 美元价格
- 提供 `sample_data`，没有接口时也可以测试界面和评分模型

## 安装

需要 Python 3.10+。

```powershell
cd C:\Users\caipi\Documents\美国工作
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

复制配置文件：

```powershell
copy .env.example .env
```

打开 `.env` 填写：

```dotenv
STOCKX_HOST=http://43.136.43.128:61030/api/stockx
STOCKX_TOKEN=你的_token
STOCKX_AUTH=你的_auth
STOCKX_CREDENTIAL_MODE=header
```

如果接口要求把 token/auth 放在 URL query 参数，把 `STOCKX_CREDENTIAL_MODE` 改成：

```dotenv
STOCKX_CREDENTIAL_MODE=query
```

如果两边都要传：

```dotenv
STOCKX_CREDENTIAL_MODE=both
```

如上游接口字段名不是 `token` / `auth`，可以在 `.env` 调整：

```dotenv
STOCKX_TOKEN_PARAM=token
STOCKX_AUTH_PARAM=auth
STOCKX_TOKEN_HEADER=token
STOCKX_AUTH_HEADER=auth
```

## 运行

双击 `run_streamlit.cmd`，或执行：

```powershell
streamlit run app.py
```

Streamlit 会在浏览器打开本地页面，一般是：

```text
http://localhost:8501
```

程序运行期间会自动每小时全量刷新一次已导入货号的接口数据。可在 `.env` 调整：

```dotenv
AUTO_FULL_SYNC_ENABLED=true
AUTO_FULL_SYNC_INTERVAL_MINUTES=60
SYNC_MAX_WORKERS=4
```

## 无接口测试

进入页面后：

1. 打开 `SKU 导入 / 同步`
2. 点击 `载入 sample_data`
3. 打开 `今日机会` 查看评分结果
4. 打开 `单尺码详情` 查看 Ask/Bid 深度、成交统计和原始 JSON

示例 SKU 清单在：

```text
sample_data/skus.csv
```

## 数据库

默认 SQLite 文件：

```text
data/stockx_arbitrage.sqlite
```

主要表：

- `sku_imports` / `sku_import_sheets` / `sku_items`：导入批次、原始 sheet、SKU 行
- `products`：商品信息、标题、品牌、发售日期、图片
- `raw_api_responses`：所有接口原始 JSON 和错误记录
- `sync_logs`：接口异常和同步日志
- `market_snapshots`：市场价、lowest ask、highest bid、last sale
- `sales_history`：成交历史
- `ask_depth`：当前报价深度
- `bid_depth`：买盘深度
- `opportunity_scores`：评分和买断建议
- `portfolio_trades`：手动买入/卖出记录

## 评分规则

每个 SKU + 尺码单独评分，总分 100：

| 模块 | 权重 |
| --- | ---: |
| 发售/上架时间 | 20 |
| 尺码销量 | 25 |
| 出价断层 Ask Depth | 25 |
| 成交价支撑 | 20 |
| 补货/供应风险 | 10 |

评级：

- 90-100：S
- 80-89：A
- 70-79：B+
- 60-69：B
- 40-59：C
- 40 以下：D

保护规则：

- 缺少完整 Ask Depth 时，最高评级不超过 B+
- 发售未满 90 天默认降级，并在机会页默认过滤
- 单尺码近 30 天销量小于 5 双时，不进入 A

## Ask Depth 计算

系统会按 `ask_price` 从低到高排序，逐层模拟买断：

- 累计买入数量
- 加权平均成本
- 买完后新最低价
- 理论断层和断层率
- 当前价格档到下一档的间距
- 预计消化周期

默认扫货条件：

- 买入数量不超过近 14 天销量的 `BUY_DEPTH_SALES_FRACTION`
- 断层率至少 5%
- 下一档价格不明显高于近 30 天 90 分位
- 预计消化周期不超过 21 天
- 扣除 `ESTIMATED_SELLER_FEE_RATE` 后仍有利润，默认按 3% 支付通道费计算，即售价 100 美金实际到手 97 美金

输出：

- `recommended_buy_qty`
- `max_buy_price`
- `weighted_avg_cost`
- `next_lowest_ask`
- `target_sell_price_low / high`
- `estimated_profit`
- `estimated_days_to_sell`

## 接口端点

客户端已封装：

- `/product_detail`
- `/product_market_info`
- `/product_size_price`
- `/product_size_market_info`
- `/product_activity_new`
- `/product_size_activity_new`
- `/product_ask_list`
- `/product_size_ask_list`
- `/product_bid_list`
- `/product_size_bid_list`

分页会自动读取常见的 `nextCursor` / `next_cursor` / `pageInfo.endCursor` / `pagination.nextCursor` 字段，并继续追加请求。

## 后续升级建议

- 按真实接口响应格式补齐字段映射
- 增加手续费、运费、税费、汇率配置
- 增加自动定时同步
- 加入 StockX/GOAT 跨平台价格差
- 增加导出机会列表 CSV
- 增加评分参数页面
