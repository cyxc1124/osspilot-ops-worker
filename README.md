# osspilot-ops-worker

OssPilot 运营生命周期 worker（Go）。按 [osspilot-ops-api](https://github.com/cyxc1124/osspilot-ops-api) 里的规则扫 RGW 删除 / abort。

表结构跟运营 API 仓对齐；本仓是拷贝，之后会漂移。改规则列或清理开关要两边一起改。

## 本地

不跑迁移（迁移只在 API 仓）：

```bash
export DATABASE_URL=postgres://osspilot:osspilot@127.0.0.1:5432/osspilot_ops?sslmode=disable
export S3_ENDPOINT=...
export RGW_ACCESS_KEY=...
export RGW_SECRET_KEY=...
# 可选：LIFECYCLE_INTERVAL=1h
go test ./...
go run ./cmd/worker
```

未设 `DATABASE_URL` 时退出。S3 凭证也可写在运营库 `system_settings`。

## 许可

AGPL-3.0-only

## 镜像

`Dockerfile` 只编 worker。入口默认 `command` 为 `worker`。
