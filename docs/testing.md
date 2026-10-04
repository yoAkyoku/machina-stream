# 測試與驗收

可重現的檢查與最新 Docker Desktop runtime 證據見 [validation status](validation-status.md)。
目前 MVP 的非 recovery、故障恢復與瀏覽器流程已在專用 Compose project 通過；processor
readiness 會等到 Kafka telemetry partition assignment，offline episode 掃描則在可取消的
背景 task 執行，避免大批 stale machine 阻塞事件消費。#9 三輪本機容量基線也已完成；結果
僅適用於報告記錄的 Docker Desktop 主機與單 broker 拓撲，不能推導跨硬體或生產 HA 承諾。

## 已約定的測試邊界

測試只從公開介面觀察行為：

1. `POST /api/v1/telemetry`：驗證 202 代表 Kafka 已確認、malformed JSON 為 400、schema/範圍錯誤為 422、broker 不可用不得回成功。
2. 唯讀查詢 API：`/api/v1/machines`、`/api/v1/events`、`/api/v1/alerts`、`/api/v1/dead-letters`。事件清單支援 `limit`/`offset` 和 factory/machine 篩選，以便按 ID 核對完整資料集。
3. 真實 Docker Compose 路徑：FastAPI、Kafka、processor、PostgreSQL 與 Grafana/Prometheus；可靠性測試不替換 Kafka 或 PostgreSQL。
4. 瀏覽器控制室：Playwright 經由實際讀取 API 驗證機台/告警呈現、唯讀界線、窄螢幕溢位，並保留桌面/手機截圖作 QA 證據。

單元測試只在 Kafka producer 這個外部邊界使用錄製器；schema、API 回應與序列化仍由真實應用程式處理。資料庫、Kafka、事件處理與告警規則在 integration/recovery 測試中使用 Compose 真實服務。

## 本機測試

`scripts/test_stack.ps1` 會建立唯一名稱的 Compose 專案，並在 loopback 上動態挑選未占用的 API／PostgreSQL 宿主機埠；不會操作一般 `machina-stream` 開發卷。結束時只移除該次測試專案的容器與卷。完整 recovery profile 會讓 processor 停止 120 秒，並讓 PostgreSQL 中斷 60 秒，預期需數分鐘。

```powershell
uv sync --locked --extra dev
uv run ruff check backend tests
uv run pytest tests/unit --strict-markers
pnpm --dir web install --frozen-lockfile
pnpm --dir web lint
pnpm --dir web typecheck
```

執行完整流程與故障恢復；腳本會先確認 `docker`/Compose v2、Node、pnpm、uv 與 Docker Desktop Linux engine，透過 `uv` 驗證並建立鎖定的 Python 3.12 環境，並執行不啟動服務的 Compose security contract，再修改本次測試專用的 process environment：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/test_stack.ps1
```

若只要較快的端到端功能檢查，可加 `-SkipRecoveryTests`；Playwright 測試可用 `-SkipBrowserTests` 略過。測試需要 Docker Desktop Linux engine、Compose v2、Python 3.12、uv、pnpm，以及 Playwright Chromium。失敗時保留終端輸出與 `web/test-results`，不可把未跑的測試標成通過。

## 最近一次 Docker Desktop 驗收

2026-09-28 的專用 project `machina-stream-e2e-all-20260928d` 使用
`SIMULATOR_MACHINE_COUNT=1`、`SIMULATOR_EVENT_INTERVAL_SECONDS=1` 與
`PROCESSOR_DATABASE_POOL_SIZE=20`，並以 `desktop-linux` engine 啟動 PostgreSQL、Kafka、Kafka
init、API、processor、web、Prometheus 與 Grafana。結果如下：

- 單元：63 passed，1.39 秒（另有一個預期的 Windows pytest cache warning）。
- 非 recovery integration：8 passed，70 deselected，5.75 秒（完整腳本同時收集 unit 與 integration）。
- recovery：7 passed，8 deselected，342.52 秒；涵蓋 simulator/API/processor/Kafka 重啟、12,000 筆 backlog、60 秒 PostgreSQL outage、DLQ audit/replay、missing-DLQ provisioning 與 DB/offset crash window。
- Chromium Playwright：1 passed，5.4 秒；桌面與行動版截圖保留於 `web/test-results/`。
- 獨立 PostgreSQL 60 秒 outage 回歸：1 passed，65.27 秒。

這是 pinned MVP 拓撲的本機 runtime 證據；專案維持單 broker，不代表 HA、生產災難復原或容量門檻已通過。`scripts/test_stack.ps1` 在驗收後已清理專用 Compose project 與 volumes，完整 logs、`ps` 與 config 證據保留於 `artifacts/compose/machina-stream-e2e-all-20260928d/`。

若公司 proxy 或 endpoint-security 會替套件 registry 換發 TLS 憑證，先將信任鏈 PEM 路徑放入
`$env:MACHINA_REGISTRY_CA_FILE`；測試腳本會自動疊加 `compose.registry-ca.yaml`，只在 BuildKit
套件安裝步驟使用該 secret，不會把憑證寫入 image layer。一般公開網路不需要這個 override。

GitHub Actions 的 Compose job 會在清理測試專案前保存 `ps`、不插值的 Compose config 與完整服務 logs 為 `machina-stream-compose-results` artifact；瀏覽器截圖與 trace 另存為 `machina-stream-browser-results`。config 保留變數表達式以避免把環境密碼寫入 artifact。這些 artifacts 是失敗診斷證據，不代表尚未通過的測試自動變成通過。

## #9：容量驗收（三輪本機基準已通過）

v1 一般負載定為 1,000 台合成機台、每台每 10 秒一筆，即 100 telemetry events/s。這與 #1 的模擬規模一致；1,000 events/s 不作 v1 正常負載門檻。k6 先 warm-up 5 分鐘，再量測 30 分鐘。至少在同一台主機跑三次，三次都必須各自通過，不能只挑最好的一次。

測試環境以每份報告記錄的實際主機為準，不宣稱跨硬體通用。每次記錄 OS/版本/架構、CPU 型號與核心數、記憶體、Docker Engine 與 Compose 版本、k6 版本、程式 commit、映像 tag/digest、Compose 資料庫/broker 設定與各容器 CPU/記憶體限制。三次 run 使用相同 commit、設定與主機；有任何差異須另開一組結果。

每次 run 的 pass/fail：

- telemetry 到達率為 100 events/s，30 分鐘至少接受 180,000 個事件，`dropped_iterations` 必須為 0；HTTP 非 202 比率 < 0.1%。
- `POST /api/v1/telemetry` 延遲 p95 < 200 ms。每份結果都報告 p50、p95、p99、最大值與實際吞吐量；p50/p99 目前只記錄，不另設門檻。
- 告警端到端 p95 < 2 秒，起點是 client 開始 POST，終點是唯讀 alerts API 首次回傳相同 `latest_event_id`；該 run 的 probe miss 必須為 0，使用同一測試主機時鐘。
- Kafka 總 consumer lag 不得連續五個一分鐘視窗上升，穩態峰值不得超過 100 筆；停止負載後 30 秒內歸零。
- 1,000、5,000、10,000 events/s 僅作探索性壓測，不自動成為 v1 pass/fail。若任何一 run 未過門檻，整組基線判為未通過；記錄失敗，不以重跑覆蓋原結果。本次三輪均通過，完整數字與環境見 [performance baseline report](performance-baseline-20260929.md)，原始 JSON/lag CSV 留在 `artifacts/performance-20260929/`。

```powershell
$runDir = "artifacts/performance-$(Get-Date -Format yyyyMMdd-HHmmss)"
New-Item -ItemType Directory -Force $runDir | Out-Null
k6 run --summary-export (Join-Path $runDir "run-1.json") tests/performance/telemetry.js
if ($LASTEXITCODE -ne 0) { throw "Run 1 failed; preserve its artifacts and stop." }
k6 run --summary-export (Join-Path $runDir "run-2.json") tests/performance/telemetry.js
if ($LASTEXITCODE -ne 0) { throw "Run 2 failed; preserve its artifacts and stop." }
k6 run --summary-export (Join-Path $runDir "run-3.json") tests/performance/telemetry.js
if ($LASTEXITCODE -ne 0) { throw "Run 3 failed; preserve its artifacts and stop." }
```

腳本內建 API p95、非 202 比率、接受事件數、告警端到端延遲和 dropped iterations 門檻，並在 JSON 摘要輸出 p50/p95/p99。每秒接受吞吐量以 baseline 的 accepted count 除以 1,800 秒計算，避免把 warm-up 算入分母。Kafka lag 的視窗趨勢需從 Grafana/Prometheus 匯出並附在量測紀錄中；本次報告已附三份 30 秒取樣 CSV。這組數字是單一 Docker Desktop 主機的本機基準，不是生產容量或 HA 聲明。

## #10：資料完整性與故障矩陣

| 故障/輸入 | 注入方式 | 通過條件 |
| --- | --- | --- |
| processor 停止 120 秒、API 繼續以約 100 events/s 接受事件 | Compose 停止 processor；每秒送 100 筆、連續 120 秒 | 12,000 個 accepted event ID 最終各有且只有一筆事件歷史；機台序號不倒退；consumer lag 在恢復後 180 秒內排空；本次 recovery run 通過 |
| PostgreSQL 停止 60 秒 | Compose 停止 postgres；API/Kafka 保持運作 | POST 仍只在 Kafka ACK 後回 202；有限次 DB retry 用盡後事件到 DLQ；PostgreSQL 回復後 audit 可見，人工重播後該 ID 恰有一筆歷史；重播不刷新在線時間；本次 recovery run 通過 |
| API/Kafka/processor 重啟與 DB/offset 崩潰窗口 | API/processor 重啟測試確認已 ACK 事件可落庫；Kafka 重啟測試在 processor 暫停時保留已 ACK 事件，恢復後再由 API 送入新事件；另由測試 worker 在指定事件的 DB commit 完成後、Kafka offset commit 前停住，再強制終止並啟動正式 worker | Kafka 重啟後 consumer 與 API producer 均可恢復；DB/offset 崩潰重播後來源 offset 前進、機台投影序號正確且事件歷史恰一筆；本次 recovery run 通過 |
| simulator 重啟 | 重啟合成資料服務 | 由唯讀 projection 取得既有序號，重啟後機台序號持續遞增；本次 recovery run 通過 |
| event ID 相同、payload 不同 | 對同一 event ID 送兩個不同 payload | 原始歷史不被覆寫；衝突送 DLQ 並可由 read API 查到；本次 integration run 通過 |
| gap、晚到/亂序、相同機台與序號不同 ID | 以相同 Kafka machine key 送遞增與非遞增 sequence | 歷史保留；最新投影不回退；gap/conflict 計數正確；本次 integration run 通過 |
| invalid JSON/schema、Poison payload | API 測試；integration 直接送 poison bytes 至 `telemetry.events.v1` | ingress invalid payload 不入 Kafka；Kafka poison 進 DLQ 且 processor 繼續處理後續事件；本次 integration/recovery run 通過 |
| DLQ 不可用 | processor 使用不存在的專屬測試 topic；確認 Kafka consumer group offset 停在 poison 記錄之前，再建立 topic | 測試要求 DLQ 稽核成功後來源 offset 才前進；本次 recovery run 通過 |
| duplicate 與 DLQ 重播 | 同一 event ID 重送；修復後執行 replay CLI | duplicate no-op；DLQ at-least-once 重複可辨識；重播保留 event ID/machine key 且不改寫已存在事件 |

目前 Compose 是單 broker 開發拓撲，不測或宣稱 broker HA；故障測試也不代表生產環境災難復原。#9 本機基線已通過，但任何 HA/跨主機聲稱仍維持未驗證。
