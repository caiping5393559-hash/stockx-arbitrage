# SpiderX StockX 热销榜接口记录

来源文档：

- https://api.spiderx.cc/api/stockx/docs#/stockx/i_search_advanced_v2_api_stockx_search_advanced_v2_post
- OpenAPI 已保存：`docs/api/spiderx_stockx_openapi.json`

接口地址：

- Host: `https://api.spiderx.cc/api/stockx`
- 热销/高级搜索：`POST /search_advanced_v2`
- 商品详情：`GET /product_detail`
- 尺码价格：`GET /product_size_price`
- 市场信息：`GET /product_market_info`

`/search_advanced_v2` 关键参数：

- `category` 必填，鞋类默认 `sneakers`
- `page` 页码，默认 1
- `currency_code` 默认 `USD`
- `country` 使用 `US`
- `sort` 可用 `deadstock_sold` 获取热销排序，也支持 `most-active`, `lowest_ask`, `release_date` 等
- `sort_order` 默认 `DESC`
- 可选：`keyword`, `brand`, `gender`, `lowest_ask_range`

当前客户端处理：

- `StockXClient.request()` 已支持 `method="POST"`
- `StockXClient.search_advanced_v2()` 默认按 `deadstock_sold DESC` 查询 sneakers 热销数据
- 所有返回原始 JSON 仍走现有 `save_raw_response()` 保存
