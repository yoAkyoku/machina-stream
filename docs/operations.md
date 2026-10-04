# 本機操作與恢復

## 健康檢查

- API liveness：`GET /health/live`
- API readiness（Kafka producer ready）：`GET /health/ready`
- Prometheus：`GET /metrics`；processor metrics 在 Compose 私有網路的 `processor:9101/metrics`
- Grafana 工程面板包含 `machina_postgres_up`（API／processor 對 PostgreSQL 的觀測）及 Python process CPU／resident memory 指標；這些是服務／容器層級訊號，不是主機或雲端容量承諾。資料庫健康探測週期可用 `MACHINA_DB_HEALTH_INTERVAL_SECONDS` 調整（預設 15 秒）。
- 本機 Grafana：`http://localhost:3000`，預設管理帳密來自 `.env`
- DLQ 稽核清單：`GET /api/v1/dead-letters`

## PostgreSQL 或 processor 中斷

API 的 202 只表示 Kafka 已確認，不表示 PostgreSQL projection 已完成。processor 最多嘗試 5 次，指數退避加 full jitter（每次等待上限依序為 1、2、4、8 秒）；之後寫入 DLQ，且只有 DLQ broker 確認後才提交原始 offset。若 DLQ broker 不可用，processor 暫停來源 partition 並保留 offset。

確認 PostgreSQL 已恢復後，先查詢 `/api/v1/dead-letters` 與 Grafana 的 processor/DLQ 指標。修正造成 poison 的條件後，依 DLQ 的 `event_id` 手動重播：

```powershell
docker compose exec processor python -m machina_stream.replay_dlq --event-id 550e8400-e29b-41d4-a716-446655440000
```

要用 DLQ 記錄的原始 topic/partition/offset 選取單筆：

```powershell
docker compose exec processor python -m machina_stream.replay_dlq --source telemetry.events.v1:2:41
```

`--all` 會重播所有尚未標記 replayed 的 DLQ 記錄，只有在逐筆確認全部原因已修復後才使用。重播會保留原 payload、event ID 與 machine key，另加 replay header；它不會刷新 machine freshness，也不會解除 offline episode。重播程序可安全重跑，資料庫事件 ID 唯一鍵負責冪等效果。

重播 CLI 只會送出來源座標與原始 base64 payload 都有效的記錄；格式錯誤的 DLQ 記錄會略過並保留在 Kafka/稽核範圍內，避免一筆壞資料中止整批掃描。

## 本機資料

一般關閉使用 `docker compose down`，PostgreSQL/Grafana/Prometheus 卷會保留。只有確認資料可丟棄時才執行 `docker compose down -v`。`scripts/test_stack.ps1` 使用專屬且隨機的 Compose project name，清理範圍只限該次測試產生的卷。
