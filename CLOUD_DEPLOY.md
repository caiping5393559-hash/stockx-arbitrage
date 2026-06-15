# 云端部署说明

目标：把当前本地 StockX / GOAT 套利扫描器部署到你的服务器，让别人通过网页访问。

## 当前云端第一版：Render + Firebase

已支持：

- Docker 部署
- 公网访问 Streamlit 页面
- 登录保护
- `.env` 读取 StockX Token/Auth，不写死
- Render 持久化磁盘保存 SQLite 本地缓存
- Firebase Admin / Firestore 连接检测
- Render Blueprint：`render.yaml`

当前不是 PostgreSQL/MySQL 路线。Firebase 是 Firestore 文档数据库，不是 SQL 数据库：

- 当前代码大量使用 SQLite 语法、`sqlite3.Connection`、`PRAGMA`、`INSERT OR REPLACE`
- 所以第一版在 Render 上保留 SQLite 作为计算缓存，同时连接 Firebase
- 第二步再把用户、配置、任务状态、结果快照逐步迁到 Firestore

## 服务器准备

Render 需要：

- 一个 Web Service
- 连接 GitHub 仓库或上传代码
- 环境变量
- Persistent Disk，挂载到 `/app/data`

## 部署步骤

1. 在 Firebase 控制台创建项目。

2. 创建 Service Account JSON。

3. 在本机把 JSON 转成 base64：

```powershell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("firebase-service-account.json"))
```

4. Render 创建 Blueprint 或 Docker Web Service。

如果使用 `render.yaml`，Render 会读取：

```text
render.yaml
```

5. Render 环境变量填写：

```dotenv
APP_LOGIN_ENABLED=true
APP_USERNAME=admin
APP_PASSWORD=换成强密码
CLOUD_STORAGE_BACKEND=firebase
FIREBASE_PROJECT_ID=你的Firebase项目ID
FIREBASE_COLLECTION_PREFIX=stockx_goat
FIREBASE_SERVICE_ACCOUNT_B64=上一步生成的base64
STOCKX_TOKEN=你的token
STOCKX_AUTH=你的auth
STOCKX_DB_PATH=/app/data/stockx_arbitrage.sqlite
```

6. Render Disk：

```text
Mount Path: /app/data
Size: 10GB 或更大
```

7. 部署后访问 Render 给你的 URL。

8. 打开「设置」，看 Firebase 是否显示“已连接”。

## 本地 Docker 测试

如果你要先在本地跑 Docker：

1. 复制云端配置：

```bash
cp .env.cloud.example .env
```

2. 编辑 `.env`：

```dotenv
APP_LOGIN_ENABLED=true
APP_USERNAME=admin
APP_PASSWORD=换成强密码
CLOUD_STORAGE_BACKEND=firebase
FIREBASE_PROJECT_ID=你的Firebase项目ID
FIREBASE_SERVICE_ACCOUNT_B64=你的base64服务账号
STOCKX_TOKEN=你的token
STOCKX_AUTH=你的auth
STOCKX_DB_PATH=/app/data/stockx_arbitrage.sqlite
```

3. 启动：

```bash
docker compose up -d --build
```

4. 浏览器访问：

```text
http://localhost:8501
```

## 反向代理建议

如果你有域名，建议 Nginx 代理到本机 `8501`，并配置 HTTPS。

示例：

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

## 下一步：把套利数据迁到 Firestore

需要做的不是“换连接字符串”，而是数据模型迁移：

- `users`
- `sku_batches`
- `sku_items`
- `products`
- `product_sizes`
- `ask_depth_snapshots`
- `bid_depth_snapshots`
- `sales_history`
- `opportunity_scores`
- `goat_consignment_items`
- `goat_consignment_scores`
- `sync_jobs`
- `raw_api_responses`

推荐生产方案：

- Render Web Service 跑前端
- Render Background Worker 跑 StockX / GOAT 同步
- Firestore 保存用户、任务、结果
- Render Disk 只作为临时缓存
- Firebase Auth 或应用内账号控制访问
