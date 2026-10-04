import http from "k6/http";
import { check, sleep } from "k6";
import execution from "k6/execution";
import { Counter, Rate, Trend } from "k6/metrics";

const eventsPerSecond = Number(__ENV.EVENTS_PER_SECOND || 100);
const machineCount = Number(__ENV.MACHINE_COUNT || 1000);
const warmupDuration = __ENV.WARMUP_DURATION || "5m";
const measuredDuration = __ENV.MEASURED_DURATION || "30m";
const warmupRate = Math.max(1, Math.floor(eventsPerSecond / 10));
const alertLatency = new Trend("machina_alert_e2e_latency_ms", true);
const alertLatencyMisses = new Counter("machina_alert_latency_misses");
const acceptedEvents = new Counter("machina_telemetry_accepted_events");
const rejectedRate = new Rate("machina_telemetry_rejected_rate");

export const options = {
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
  scenarios: {
    warmup: {
      executor: "constant-arrival-rate",
      rate: warmupRate,
      timeUnit: "1s",
      duration: warmupDuration,
      preAllocatedVUs: 50,
      maxVUs: 250,
      tags: { profile: "warmup" },
    },
    baseline: {
      executor: "constant-arrival-rate",
      startTime: warmupDuration,
      rate: eventsPerSecond,
      timeUnit: "1s",
      duration: measuredDuration,
      preAllocatedVUs: 150,
      maxVUs: 1000,
      tags: { profile: "baseline" },
    },
  },
  thresholds: {
    'http_req_duration{profile:baseline,name:telemetry_ingest}': ["p(95)<200"],
    'http_req_failed{profile:baseline,name:telemetry_ingest}': ["rate<0.001"],
    'machina_telemetry_rejected_rate{profile:baseline}': ["rate<0.001"],
    'machina_telemetry_accepted_events{profile:baseline}': ["count>=180000"],
    'machina_alert_e2e_latency_ms{profile:baseline}': ["p(95)<2000"],
    'machina_alert_latency_misses{profile:baseline}': ["count==0"],
    'dropped_iterations{profile:baseline}': ["count==0"],
  },
};

function uuidV4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (character) => {
    const value = Math.floor(Math.random() * 16);
    return (character === "x" ? value : (value & 0x3) | 0x8).toString(16);
  });
}

function timestamp() {
  return new Date().toISOString();
}

export default function submitTelemetry() {
  const profile = execution.scenario.name === "baseline" ? "baseline" : "warmup";
  const iteration = execution.scenario.iterationInTest;
  const machineIndex = iteration % machineCount;
  const machineId = `machine-load-${profile}-${String(machineIndex).padStart(4, "0")}`;
  const sequence = Math.floor(iteration / machineCount) + 1;
  const alertProbe = profile === "baseline" && machineIndex === 0;
  const event = {
    event_id: uuidV4(),
    schema_version: 1,
    factory_id: "factory-load",
    line_id: "line-load",
    machine_id: machineId,
    sequence_no: sequence,
    event_time: timestamp(),
    metrics: {
      temperature_c: alertProbe ? 98 : 68,
      vibration_rms_mm_s: 2.1,
      current_a: 55,
    },
    machine_status: "running",
    error_code: null,
  };
  const base = __ENV.MACHINA_API_URL || "http://127.0.0.1:18000";
  const startedAt = Date.now();
  const response = http.post(`${base}/api/v1/telemetry`, JSON.stringify(event), {
    headers: { "Content-Type": "application/json" },
    tags: { name: "telemetry_ingest", profile },
  });
  const accepted = check(response, { "telemetry accepted": (res) => res.status === 202 });
  rejectedRate.add(response.status !== 202, { profile });
  if (accepted) acceptedEvents.add(1, { profile });
  if (!accepted || !alertProbe) return;

  const deadline = Date.now() + 2000;
  while (Date.now() < deadline) {
    const alerts = http.get(`${base}/api/v1/alerts?active_only=true&limit=500`, {
      tags: { name: "alert_read", profile },
    });
    if (alerts.status === 200) {
      const items = alerts.json("items");
      if (Array.isArray(items) && items.some((item) => item.latest_event_id === event.event_id)) {
        alertLatency.add(Date.now() - startedAt, { profile });
        return;
      }
    }
    sleep(0.1);
  }
  alertLatencyMisses.add(1, { profile });
}
