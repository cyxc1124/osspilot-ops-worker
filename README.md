# osspilot-ops-worker

OssPilot 运营生命周期 worker（Go）。连**运营库**，按 [osspilot-ops-api](https://github.com/cyxc1124/osspilot-ops-api) 的 `lifecycle_rules` 扫 RGW：到期对象删除、过期分片 abort。

不用 Redis / asynq，进程内 ticker（默认 1 小时，`LIFECYCLE_INTERVAL`，最短 1 分钟）。启动先跑一轮。

不管清单、回收站/版本的平台设置清理、请求统计、批量复制移动——那些在 [osspilot-tenant-worker](https://github.com/cyxc1124/osspilot-tenant-worker)。

表结构跟运营 API 仓对齐；本仓是拷贝，之后会漂移。改规则列或清理开关要两边一起改。

## 一轮做什么

`system_settings.lifecycle_cleanup_enabled` 为关，或没有 S3 凭证时整轮跳过。S3 先看运营库设置，空则回退 `S3_ENDPOINT` / `RGW_ACCESS_KEY` / `RGW_SECRET_KEY`。

对每条启用规则、按前缀：

| 规则字段 | 动作 |
| --- | --- |
| `delete_after_days` | 删 live 对象（跳过 `.trash/`、`.versions/`） |
| `cleanup_trash_after_days` | 删 `.trash/` 下到期对象 |
| `cleanup_versions_after_days` | 删 `.versions/` 下到期对象 |
| `cleanup_multipart_after_days` | abort 过期分片 |

每条动作最多删 2000 个。某个桶这轮真删过对象时，用 `TENANT_API_URL` + `PROJECTION_SECRET` 打租户 `POST /internal/buckets/{name}/inventory`，让租户 worker 补清单。没配这两项只打日志。

`GET /healthz` 默认 `:8080`（`HTTP_ADDR`）。

日志走 stdout（`log/slog`）。`LOG_LEVEL=debug|info|warn|error`（默认 info），`LOG_FORMAT=text|json`（默认 text）。每轮有 `lifecycle run start` / `lifecycle run done`。

## 本地

不跑迁移（迁移只在 API 仓）：

```bash
export DATABASE_URL=postgres://osspilot:osspilot@127.0.0.1:5432/osspilot_ops?sslmode=disable
export S3_ENDPOINT=...
export RGW_ACCESS_KEY=...
export RGW_SECRET_KEY=...
# 删对象后入队租户清单：TENANT_API_URL / PROJECTION_SECRET
# 可选：LIFECYCLE_INTERVAL=1h
go test ./...
go run ./cmd/worker
```

未设 `DATABASE_URL` 时退出。

## 许可

AGPL-3.0-only

## 镜像

`Dockerfile` 只编 worker。入口默认 `command` 为 `worker`。
