"use client";

import { useEffect, useMemo, useState } from "react";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://127.0.0.1:18000";

type Machine = {
  factory_id: string;
  line_id: string;
  machine_id: string;
  sequence_no: number;
  latest_event_id: string | null;
  event_time: string | null;
  last_seen_at: string | null;
  online: boolean;
  metrics: {
    temperature_c?: number;
    vibration_rms_mm_s?: number;
    current_a?: number;
  } | null;
  machine_status: "running" | "idle" | "stopped" | "fault" | null;
  error_code: string | null;
  sequence_gap_count: number;
  sequence_conflict_count: number;
};

type Alert = {
  episode_id: string;
  factory_id: string;
  line_id: string;
  machine_id: string;
  rule_id: string;
  rule_version: string;
  severity: "warning" | "critical";
  status: "OPEN" | "RESOLVED";
  opened_at: string;
  last_seen_at: string;
  occurrence_count: number;
  latest_value: number | null;
  diagnostic_code: string | null;
};

type EventRecord = {
  event_id: string;
  machine_id: string;
  sequence_no: number;
  event_time: string;
  received_at: string;
  sequence_outcome: "projected" | "gap" | "sequence_conflict" | "out_of_order";
  replayed: boolean;
};

type ApiList<T> = { items: T[]; total: number };

function clockText(value: string | null): string {
  if (!value) return "—";
  return new Intl.DateTimeFormat("zh-TW", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function numeric(value: number | undefined, digits = 1): string {
  return typeof value === "number" ? value.toFixed(digits) : "—";
}

function machineState(machine: Machine): { label: string; tone: string } {
  if (!machine.online) return { label: "離線", tone: "offline" };
  if (machine.machine_status === "fault") return { label: "故障", tone: "fault" };
  if (machine.machine_status === "idle") return { label: "待機", tone: "idle" };
  if (machine.machine_status === "stopped") return { label: "停止", tone: "idle" };
  return { label: "運作中", tone: "online" };
}

function alertTitle(ruleId: string): string {
  const titles: Record<string, string> = {
    temperature_high: "溫度偏高",
    vibration_high: "振動偏高",
    current_high: "電流偏高",
    machine_fault: "機台故障",
    machine_offline: "機台離線",
  };
  return titles[ruleId] ?? ruleId;
}

function outcomeLabel(outcome: EventRecord["sequence_outcome"]): string {
  const labels = {
    projected: "已更新狀態",
    gap: "序號有缺口",
    sequence_conflict: "序號衝突",
    out_of_order: "較晚抵達",
  };
  return labels[outcome];
}

export default function Home() {
  const [machines, setMachines] = useState<Machine[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [events, setEvents] = useState<EventRecord[]>([]);
  const [updatedAt, setUpdatedAt] = useState<Date | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedLine, setSelectedLine] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    let controller: AbortController | null = null;

    const refresh = async () => {
      controller?.abort();
      controller = new AbortController();
      try {
        const options = { cache: "no-store" as RequestCache, signal: controller.signal };
        const [machineResponse, alertResponse, eventResponse] = await Promise.all([
          fetch(`${API_BASE}/api/v1/machines?limit=1000`, options),
          fetch(`${API_BASE}/api/v1/alerts?active_only=true&limit=100`, options),
          fetch(`${API_BASE}/api/v1/events?limit=24`, options),
        ]);
        if (!machineResponse.ok || !alertResponse.ok || !eventResponse.ok) {
          throw new Error("監測 API 暫時無法讀取，請確認本機服務仍在運作。 ");
        }
        const [machineList, alertList, eventList] = (await Promise.all([
          machineResponse.json(),
          alertResponse.json(),
          eventResponse.json(),
        ])) as [ApiList<Machine>, ApiList<Alert>, ApiList<EventRecord>];
        if (!alive) return;
        setMachines(machineList.items);
        setAlerts(alertList.items);
        setEvents(eventList.items);
        setUpdatedAt(new Date());
        setError(null);
      } catch (cause) {
        if (!alive || (cause instanceof DOMException && cause.name === "AbortError")) return;
        setError(cause instanceof Error ? cause.message : "監測 API 暫時無法讀取。 ");
      }
    };

    void refresh();
    const timer = window.setInterval(refresh, 5000);
    return () => {
      alive = false;
      window.clearInterval(timer);
      controller?.abort();
    };
  }, []);

  const lineSummary = useMemo(() => {
    const groups = new Map<string, { machines: number; online: number; alerts: number }>();
    for (const machine of machines) {
      const group = groups.get(machine.line_id) ?? { machines: 0, online: 0, alerts: 0 };
      group.machines += 1;
      if (machine.online) group.online += 1;
      groups.set(machine.line_id, group);
    }
    for (const alert of alerts) {
      const group = groups.get(alert.line_id) ?? { machines: 0, online: 0, alerts: 0 };
      group.alerts += 1;
      groups.set(alert.line_id, group);
    }
    return [...groups.entries()].sort(([left], [right]) => left.localeCompare(right));
  }, [machines, alerts]);

  const visibleMachines = useMemo(
    () => (selectedLine ? machines.filter((machine) => machine.line_id === selectedLine) : machines),
    [machines, selectedLine],
  );
  const onlineCount = machines.filter((machine) => machine.online).length;
  const criticalCount = alerts.filter((alert) => alert.severity === "critical").length;

  return (
    <main className="shell">
      <aside className="sidebar" aria-label="主要導覽">
        <a className="brand" href="#overview" aria-label="Machina Stream 總覽">
          <span className="brand-mark" aria-hidden="true"><i /><i /><i /></span>
          <span><strong>Machina</strong><small>Stream</small></span>
        </a>
        <div className="site-picker">
          <span className="site-glyph" aria-hidden="true">M</span>
          <span><strong>示範工廠</strong><small>台灣 · 合成資料</small></span>
          <span className="chevron" aria-hidden="true">⌄</span>
        </div>
        <nav className="nav-list">
          <a className="nav-link active" href="#overview"><span className="nav-icon">◫</span>總覽</a>
          <a className="nav-link" href="#machines"><span className="nav-icon">▦</span>機台 <span className="nav-count">{machines.length}</span></a>
          <a className="nav-link" href="#alerts"><span className="nav-icon">◇</span>告警 <span className="nav-count alert-count">{alerts.length}</span></a>
          <a className="nav-link" href="#events"><span className="nav-icon">≋</span>事件紀錄</a>
        </nav>
        <div className="sidebar-bottom">
          <div className="sidebar-note"><span className="note-dot" />唯讀監測<br /><small>不連接實體機台</small></div>
          <a className="sidebar-docs" href={`${API_BASE}/docs`} target="_blank" rel="noreferrer">API 文件 <span>↗</span></a>
          <div className="build-tag">MACHINA STREAM <span>v0.1</span></div>
        </div>
      </aside>

      <section className="workspace" id="overview">
        <header className="topbar">
          <div className="crumb"><span>工廠監測</span><b>/</b><strong>即時總覽</strong></div>
          <div className="topbar-status">
            <span className={`connection-dot ${error ? "stale" : ""}`} />
            <span>{error ? "資料更新中斷" : "即時串流"}</span>
            <span className="topbar-divider" />
            <span className="refresh-label">5 秒更新</span>
          </div>
        </header>

        <div className="page-content">
          <section className="page-heading">
            <div>
              <p className="eyebrow">合成產線監測</p>
              <h1>工廠總覽</h1>
              <p className="heading-copy">掌握機台狀態、事件流與需要留意的變化。</p>
            </div>
            <div className="snapshot-meta">
              <span className="snapshot-label">最後更新</span>
              <strong>{updatedAt ? clockText(updatedAt.toISOString()) : "等待連線"}</strong>
              <span>伺服器接收時間為新鮮度依據</span>
            </div>
          </section>

          {error && <div className="error-banner" role="status">{error}</div>}

          <section className="summary-line" aria-label="工廠摘要">
            <div><strong>{machines.length.toLocaleString("zh-TW")}</strong><span>台機台</span></div>
            <span className="summary-separator" />
            <div><strong className="tone-online">{onlineCount.toLocaleString("zh-TW")}</strong><span>在線</span></div>
            <span className="summary-separator" />
            <div><strong className={alerts.length ? "tone-warning" : ""}>{alerts.length}</strong><span>活動告警</span></div>
            <span className="summary-separator" />
            <div><strong className={criticalCount ? "tone-critical" : ""}>{criticalCount}</strong><span>嚴重</span></div>
            <span className="summary-tail">資料僅供展示與測試，不是安全規格</span>
          </section>

          <section className="line-section" aria-labelledby="lines-title">
            <div className="section-heading compact-heading">
              <div><h2 id="lines-title">產線狀態</h2><span>選取產線以篩選機台</span></div>
              {selectedLine && <button className="text-button" onClick={() => setSelectedLine(null)}>清除篩選</button>}
            </div>
            {lineSummary.length === 0 ? (
              <div className="inline-empty">尚無產線事件，合成資料啟動後會自動出現。</div>
            ) : (
              <div className="line-track">
                {lineSummary.map(([line, summary]) => {
                  const share = summary.machines ? summary.online / summary.machines : 0;
                  return (
                    <button
                      className={`line-segment ${selectedLine === line ? "selected" : ""}`}
                      key={line}
                      onClick={() => setSelectedLine(selectedLine === line ? null : line)}
                      aria-pressed={selectedLine === line}
                    >
                      <span className="line-segment-head"><strong>{line}</strong><span>{summary.online}/{summary.machines}</span></span>
                      <span className="line-track-bar" aria-hidden="true">
                        <i style={{ width: `${Math.round(share * 100)}%` }} />
                      </span>
                      <span className="line-segment-foot">
                        <span>{summary.online === summary.machines ? "運作穩定" : "有機台離線"}</span>
                        {summary.alerts > 0 && <b>{summary.alerts} 告警</b>}
                      </span>
                    </button>
                  );
                })}
              </div>
            )}
          </section>

          <div className="operations-grid">
            <section className="panel machine-panel" id="machines" aria-labelledby="machines-title">
              <div className="section-heading panel-heading">
                <div><h2 id="machines-title">機台狀態</h2><span>{selectedLine ?? "所有產線"} · {visibleMachines.length} 台</span></div>
                <span className="live-indicator"><i />更新中</span>
              </div>
              <div className="table-scroll">
                <table className="machine-table">
                  <thead><tr>
                    <th>機台</th><th>狀態</th><th>溫度</th><th>振動</th><th>電流</th><th>序號</th><th>最近收件</th>
                  </tr></thead>
                  <tbody>
                    {visibleMachines.slice(0, 100).map((machine) => {
                      const state = machineState(machine);
                      return <tr key={machine.machine_id}>
                        <td><strong className="machine-name">{machine.machine_id}</strong><small>{machine.line_id}</small></td>
                        <td><span className={`state-pill ${state.tone}`}><i />{state.label}</span></td>
                        <td className="numeric-cell">{numeric(machine.metrics?.temperature_c)}<small>°C</small></td>
                        <td className="numeric-cell">{numeric(machine.metrics?.vibration_rms_mm_s)}<small>mm/s</small></td>
                        <td className="numeric-cell">{numeric(machine.metrics?.current_a)}<small>A</small></td>
                        <td className="mono-cell">{machine.sequence_no}</td>
                        <td className="time-cell">{clockText(machine.last_seen_at)}</td>
                      </tr>;
                    })}
                  </tbody>
                </table>
                {visibleMachines.length === 0 && <div className="empty-state"><span className="empty-mark">⌁</span><strong>等待第一筆機台事件</strong><p>合成資料會透過 API 逐步建立機台清單。</p></div>}
                {visibleMachines.length > 100 && <div className="table-footnote">目前顯示前 100 台；完整清單可由 API 查詢。</div>}
              </div>
            </section>

            <section className="panel alerts-panel" id="alerts" aria-labelledby="alerts-title">
              <div className="section-heading panel-heading">
                <div><h2 id="alerts-title">活動告警</h2><span>依嚴重度與最近觸發排序</span></div>
                <span className="alert-total">{alerts.length}</span>
              </div>
              <div className="alert-list">
                {alerts.slice(0, 12).map((alert) => <article className={`alert-row ${alert.severity}`} key={alert.episode_id}>
                  <span className="severity-mark" aria-hidden="true" />
                  <div className="alert-copy">
                    <div className="alert-title-line"><strong>{alertTitle(alert.rule_id)}</strong><span className={`severity-label ${alert.severity}`}>{alert.severity === "critical" ? "嚴重" : "注意"}</span></div>
                    <span className="alert-machine">{alert.machine_id} <i>/</i> {alert.line_id}</span>
                    <span className="alert-meta">最近事件 {clockText(alert.last_seen_at)}{alert.occurrence_count > 1 ? ` · ${alert.occurrence_count} 次觸發` : ""}</span>
                  </div>
                  {alert.latest_value !== null && <span className="alert-value">{numeric(alert.latest_value)}<small>{alert.rule_id === "temperature_high" ? "°C" : alert.rule_id === "vibration_high" ? "mm/s" : "A"}</small></span>}
                </article>)}
                {alerts.length === 0 && <div className="empty-state alert-empty"><span className="calm-mark">✓</span><strong>目前沒有活動告警</strong><p>新的告警會在此顯示，歷史紀錄仍保留。</p></div>}
              </div>
              <div className="panel-note"><span>ⓘ</span>門檻為可調整的合成展示值，非實機規格。</div>
            </section>
          </div>

          <section className="panel event-panel" id="events" aria-labelledby="events-title">
            <div className="section-heading panel-heading">
              <div><h2 id="events-title">最近事件</h2><span>依服務端收件順序顯示 · 保留遲到與亂序記錄</span></div>
              <span className="event-count">{events.length} 筆</span>
            </div>
            <div className="table-scroll">
              <table className="event-table">
                <thead><tr><th>收件時間</th><th>機台</th><th>序號</th><th>事件時間</th><th>處理結果</th><th>事件 ID</th></tr></thead>
                <tbody>
                  {events.map((event) => <tr key={`${event.event_id}-${event.received_at}`}>
                    <td className="time-cell">{clockText(event.received_at)}</td>
                    <td><strong className="machine-name">{event.machine_id}</strong></td>
                    <td className="mono-cell">{event.sequence_no}</td>
                    <td className="time-cell">{clockText(event.event_time)}</td>
                    <td><span className={`outcome-label ${event.sequence_outcome}`}>{outcomeLabel(event.sequence_outcome)}</span>{event.replayed && <small className="replayed-label">重播</small>}</td>
                    <td className="event-id">{event.event_id}</td>
                  </tr>)}
                </tbody>
              </table>
              {events.length === 0 && <div className="inline-empty">事件進入系統後會顯示在這裡。</div>}
            </div>
          </section>

          <footer className="footer-note">
            <span>Machina Stream 是合成資料示範，沒有登入、人工確認、外部通知或機台控制功能。</span>
            <span>{updatedAt ? `資料快照 ${clockText(updatedAt.toISOString())}` : "尚未取得資料快照"}</span>
          </footer>
        </div>
      </section>
    </main>
  );
}
