# 成交预测样本批次台

FastAPI + SQLite 的样本工程服务：分析员在首页选择**已入库用户**、填写**观察分钟数**
开批；观察期到期后按批号结批，系统发布一份每用户一行的完整 CSV，包含各渠道
触点数与成交标签（有成交 = 1，否则 = 0）。

## 核心快照语义

系统不依赖"当前数据"重新计算样本，所有范围在受理时刻冻结：

- **开批即冻结**：开批事务内固定名单、观察起点（开批受理时刻）与观察终点
  （起点 + 观察分钟数），并一次性算好每个用户在各渠道的触点数。
- **触点特征窗口**：只统计开批受理时**已入库**、且事件发生在此前 **7 天**
  `[开批时刻 - 7天, 开批时刻]`（两端都含）的触点，按用户 × 渠道计数。
  之后才入库的触点即使发生在这 7 天内，也**不会补进**样本特征。
- **结批标签窗口**：到期后按批号结批，只统计**首次受理结批时已入库**、且成交
  发生在观察期 `[观察起点, 观察终点)`（含起点、不含终点）的成交。有成交标 1，
  否则标 0。结批受理之后才入库的成交不改变标签。
- **受理时钟严格递增**：每次入库 / 开批 / 结批受理在事务内取一个带单调后缀的
  提交时刻（`commit_clock` 表），即使墙钟在同一微秒或被冻结，"已入库"的先后
  判定也是确定的。
- **失败与并发安全**：结批是状态机上的比较并置（open → closing → published /
  failed）。并发结批由同进程锁串行化，所有调用者拿到同一份结果；生成失败可
  重试，重试沿用**首次受理**的 `first_close_at`，范围不变；进程重启后遇到遗留的
  `closing` 状态也会沿用原受理时间继续完成。
- **唯一 CSV**：同一批号只发布一份完整 CSV，原样存库、按批号下载，重复下载字节
  一致。生成样本期间新事件仍可正常提交入库，互不阻塞。

## 启动

### Docker Compose

```bash
docker compose up --build
# 打开 http://localhost:8000
```

数据保存在命名卷 `sample_batch_data`（容器内 `/data/sample_batch.db`），
`docker compose down` 不会删除数据；需要清空时执行 `docker compose down -v`。

默认端口 8000，可通过环境变量覆盖：

```bash
SAMPLE_BATCH_PORT=8080 docker compose up --build
```

### 本地运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### 测试

```bash
pip install -r requirements.txt httpx pytest
python3 -m pytest
```

测试用可冻结时钟（`tests/conftest.py`）确定性地验证窗口边界、迟到数据、
并发结批、失败重试与重启恢复。

## 页面用法

1. **提交事件**：粘贴同一用户的触点 / 成交 JSON 并提交，事件以受理时刻入库。
2. **开批**：勾选一个或多个已入库用户，填写观察分钟数后开批。名单不存在的用户
   会被整批拒绝（404）。
3. **结批与下载**：批次列表展示状态与观察起止时刻；到期前结批返回 409
   `batch_not_due`；到期后点"结批"，发布后可下载 `{批号}.csv`。

## API

### `POST /api/events` — 提交一批已受理事件

```json
{
  "user_id": "u_1001",
  "touchpoints": [
    {"id": "tp1", "occurred_at": "2026-11-20T09:00:00Z", "channel": "搜索广告"},
    {"id": "tp2", "occurred_at": "2026-11-20T15:00:00Z", "channel": "社交媒体"}
  ],
  "conversions": [
    {"id": "cv1", "occurred_at": "2026-11-21T12:20:00Z"}
  ]
}
```

- 触点字段：`id`、`occurred_at`（ISO 8601，无时区按 UTC）、`channel`。
- 成交字段：`id`、`occurred_at`。
- `(user_id, event_id)` 幂等：内容一致的重复提交只计 `duplicates_ignored`；
  同 ID 内容冲突整批 409 `event_id_conflict`，不写入任何数据。
- 任一字段非法或批次为空，整批 422 拒绝。

### `POST /api/batches` — 开批

```json
{"user_ids": ["u_1001", "u_1002"], "observation_minutes": 60}
```

响应包含批号、状态、观察起止时刻、渠道清单与每个用户冻结的特征：

```json
{
  "batch_id": "B000001",
  "status": "open",
  "observation_start": "2026-11-21T12:00:00.000000Z",
  "observation_end":   "2026-11-21T13:00:00.000000Z",
  "channels": ["搜索广告", "社交媒体"],
  "members": [
    {"user_id": "u_1001", "features": {"搜索广告": 2, "社交媒体": 1}}
  ]
}
```

### 结批与下载

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/batches/{batch_id}/close` | 到期结批；幂等，重试 / 并发 / 重启均沿用首次受理范围 |
| `GET`  | `/api/batches/{batch_id}/csv` | 下载唯一 CSV（UTF-8 带 BOM，Excel 可直接打开） |
| `GET`  | `/api/batches` | 批次列表 |
| `GET`  | `/api/batches/{batch_id}` | 批次详情（含冻结特征） |
| `GET`  | `/api/users` | 已入库用户 |
| `GET`  | `/api/users/{user_id}/events` | 用户原始事件 |
| `GET`  | `/health` | 健康检查 |

发布后的 CSV 形如：

```csv
user_id,搜索广告,社交媒体,label
u_1001,2,1,1
u_1002,0,1,0
```

- 渠道列取开批时所有成员出现过的渠道并按名称排序；某用户没有该渠道触点则补 0。
- 每个名单用户恰好一行，行顺序按 `user_id` 排序。
- 未发布时下载返回 409 `batch_not_published`。

### 错误格式

```json
{
  "error": {
    "code": "invalid_payload",
    "message": "请求校验失败，整批请求均未被处理。",
    "details": [{"field": "body.user_ids", "message": "..."}]
  }
}
```

常见错误码：`invalid_payload`（422）、`duplicate_event_id`（422）、
`event_id_conflict`（409）、`user_not_found`（404）、`batch_not_found`（404）、
`batch_not_due`（409）、`batch_not_published`（409）、
`batch_generation_failed`（500，可安全重试）、`payload_too_large`（413）。

## 数据模型与持久化

- SQLite（WAL 模式），所有写操作走 `BEGIN IMMEDIATE`，幂等检查与插入、
  快照读取与批次落库都在同一事务内原子完成。
- `events.ingested_at` 记录受理提交时刻；批次表保存开批 / 首次结批受理时刻、
  冻结的渠道清单、成员特征 JSON 与最终 CSV 文本，重启后状态完整可恢复。
- 结批状态机：`open → closing → published`，生成异常置 `failed` 并保留原
  `first_close_at`，重试回到 `closing` 继续。

## 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SAMPLE_BATCH_DB_PATH` | `data/sample_batch.db` | SQLite 文件路径 |
| `SAMPLE_BATCH_MAX_BODY_BYTES` | `16777216` | 请求体上限（字节） |
| `SAMPLE_BATCH_PORT` | `8000` | 仅 compose 端口映射使用 |
