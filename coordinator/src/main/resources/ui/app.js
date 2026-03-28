/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

const proofListEl = document.getElementById("proof-list");
const refreshButton = document.getElementById("refresh-button");
const heroTitleEl = document.getElementById("hero-title");
const heroSubtitleEl = document.getElementById("hero-subtitle");
const heroStatusEl = document.getElementById("hero-status");
const heroProgressValueEl = document.getElementById("hero-progress-value");
const heroProgressDetailEl = document.getElementById("hero-progress-detail");
const heroProgressFillEl = document.getElementById("hero-progress-fill");
const emptyStateEl = document.getElementById("empty-state");
const detailsViewEl = document.getElementById("details-view");
const proofJsonLinkEl = document.getElementById("proof-json-link");
const clusterTargetsEl = document.getElementById("cluster-targets");

const metricEls = {
  verified: document.getElementById("metric-verified"),
  missed: document.getElementById("metric-missed"),
  outOfOrders: document.getElementById("metric-out-of-orders"),
  duplicates: document.getElementById("metric-duplicates"),
  errors: document.getElementById("metric-errors"),
  timeouts: document.getElementById("metric-timeouts")
};

const performanceMetricEls = {
  publishRate: document.getElementById("perf-publish-rate"),
  consumeRate: document.getElementById("perf-consume-rate"),
  publishErrorRate: document.getElementById("perf-publish-error-rate"),
  backlog: document.getElementById("perf-backlog"),
  publishLatencyP95: document.getElementById("perf-publish-latency-p95"),
  publishLatencyP99: document.getElementById("perf-publish-latency-p99"),
  endToEndLatencyP95: document.getElementById("perf-e2e-latency-p95"),
  endToEndLatencyP99: document.getElementById("perf-e2e-latency-p99")
};

const proofConfigEl = document.getElementById("proof-config");
const proofCheckpointsEl = document.getElementById("proof-checkpoints");
const proofPerformanceEl = document.getElementById("proof-performance");
const proofFlowEl = document.getElementById("proof-flow");
const publishLatencyLadderEl = document.getElementById("publish-latency-ladder");
const e2eLatencyLadderEl = document.getElementById("e2e-latency-ladder");
const throughputChartEl = document.getElementById("throughput-chart");
const backlogChartEl = document.getElementById("backlog-chart");
const publishLatencyChartEl = document.getElementById("publish-latency-chart");
const e2eLatencyChartEl = document.getElementById("e2e-latency-chart");

let proofs = [];
let selectedProofId = new URLSearchParams(window.location.search).get("proofId");

function formatValue(value) {
  if (value === null || value === undefined || value === "") {
    return "N/A";
  }
  if (Array.isArray(value)) {
    return value.join(", ");
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll("\"", "&quot;")
    .replaceAll("'", "&#39;");
}

function isStructuredValue(value) {
  return value !== null && typeof value === "object";
}

function formatNumber(value) {
  if (typeof value === "number") {
    return new Intl.NumberFormat().format(value);
  }
  return formatValue(value);
}

function formatDecimal(value) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "N/A";
  }
  return new Intl.NumberFormat(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2
  }).format(value);
}

function formatDuration(seconds) {
  if (typeof seconds !== "number" || Number.isNaN(seconds) || seconds < 0) {
    return "N/A";
  }

  const totalSeconds = Math.round(seconds);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const remainingSeconds = totalSeconds % 60;

  if (hours > 0) {
    return `${hours}h ${minutes}m ${remainingSeconds}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${remainingSeconds}s`;
  }
  return `${remainingSeconds}s`;
}

function setSelectedProofId(proofId) {
  selectedProofId = proofId;
  const url = new URL(window.location.href);
  if (proofId) {
    url.searchParams.set("proofId", proofId);
  } else {
    url.searchParams.delete("proofId");
  }
  window.history.replaceState({}, "", url);
}

function renderKeyValueList(target, entries) {
  target.innerHTML = "";
  entries.forEach(([key, value]) => {
    const dt = document.createElement("dt");
    dt.textContent = key;

    const dd = document.createElement("dd");
    if (isStructuredValue(value)) {
      const pre = document.createElement("pre");
      pre.className = "inline-json";
      pre.textContent = JSON.stringify(value, null, 2);
      dd.appendChild(pre);
    } else {
      dd.textContent = formatValue(value);
    }

    target.appendChild(dt);
    target.appendChild(dd);
  });
}

function formatNamedResource(resource) {
  if (!resource || typeof resource !== "object") {
    return "N/A";
  }
  const pieces = [];
  if (resource.cpu) {
    pieces.push(`CPU ${resource.cpu}`);
  }
  if (resource.memory) {
    pieces.push(`Memory ${resource.memory}`);
  }
  if (resource.storage) {
    pieces.push(`Storage ${resource.storage}`);
  }
  return pieces.length > 0 ? pieces.join(" / ") : "N/A";
}

function formatResourceBlock(resource) {
  if (!resource || typeof resource !== "object") {
    return "N/A";
  }

  if (resource.limits) {
    return formatNamedResource(resource.limits);
  }
  return "N/A";
}

function formatStorageBlock(block) {
  if (!block || typeof block !== "object") {
    return null;
  }

  const parts = [];
  if (block.storageClassName) {
    parts.push(block.storageClassName);
  }
  if (block.requests?.storage) {
    parts.push(block.requests.storage);
  }
  return parts.length > 0 ? parts.join(" / ") : null;
}

function formatJvmOptions(jvmOptions) {
  if (!jvmOptions || typeof jvmOptions !== "object" || Object.keys(jvmOptions).length === 0) {
    return "N/A";
  }
  return Object.entries(jvmOptions)
    .map(([key, value]) => `${key}=${value}`)
    .join(" · ");
}

function renderResourceCard(title, resource) {
  if (!resource || typeof resource !== "object") {
    return "";
  }

  const storageLines = [
    ["Storage", formatStorageBlock(resource.storage)],
    ["Journal", formatStorageBlock(resource.journal)],
    ["Ledger", formatStorageBlock(resource.ledger)],
    ["Data", formatStorageBlock(resource.data)],
    ["Data Log", formatStorageBlock(resource.dataLog)]
  ].filter(([, value]) => value);

  const detailRows = [
    ["Replicas", resource.replicas ?? "N/A"],
    ["Limit", formatResourceBlock(resource.resources)],
    ["JVM", formatJvmOptions(resource.jvmOptions)],
    ["Coordinator Limit", formatResourceBlock(resource.coordinatorResources)]
  ].filter(([, value]) => value && value !== "N/A");

  const allRows = [...detailRows, ...storageLines];
  if (allRows.length === 0) {
    return "";
  }

  return `
    <article class="resource-card">
      <h4>${escapeHtml(title)}</h4>
      <dl class="compact-list">
        ${allRows.map(([key, value]) => `
          <dt>${escapeHtml(key)}</dt>
          <dd>${escapeHtml(formatValue(value))}</dd>
        `).join("")}
      </dl>
    </article>
  `;
}

function renderPulsarConfig(config) {
  if (!config || typeof config !== "object" || Object.keys(config).length === 0) {
    return `<p class="cluster-empty">No custom pulsar config attached to this driver.</p>`;
  }

  const rows = Object.entries(config)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `
      <div class="config-row">
        <span class="config-key">${escapeHtml(key)}</span>
        <span class="config-value">${escapeHtml(formatValue(value))}</span>
      </div>
    `)
    .join("");

  return `<div class="config-list">${rows}</div>`;
}

function renderClusterTargets(targets) {
  if (!Array.isArray(targets) || targets.length === 0) {
    clusterTargetsEl.innerHTML = `<p class="cluster-empty">No cluster target information is attached to this run yet.</p>`;
    return;
  }

  clusterTargetsEl.innerHTML = targets.map((target) => {
    const metadata = target.metadata || {};
    const clusterResources = metadata.clusterResources || {};
    const pulsarConfig = metadata.pulsarConfig || {};
    const endpoints = target.endpoints || {};

    const resourceCards = [
      renderResourceCard("Broker", clusterResources.broker),
      renderResourceCard("BookKeeper", clusterResources.bookkeeper),
      renderResourceCard("ZooKeeper", clusterResources.zookeeper),
      renderResourceCard("Oxia", clusterResources.oxia)
    ].filter(Boolean).join("");

    const endpointRows = Object.entries(endpoints)
      .map(([key, value]) => `
        <div class="endpoint-row">
          <span class="endpoint-key">${escapeHtml(key)}</span>
          <span class="endpoint-value">${escapeHtml(formatValue(value))}</span>
        </div>
      `)
      .join("");

    return `
      <section class="target-card">
        <div class="target-card-header">
          <div>
            <p class="target-role">${escapeHtml(target.role || "default target")}</p>
            <h4>${escapeHtml(target.driverName || "Unnamed driver")}</h4>
          </div>
          <span class="target-type">${escapeHtml(target.driverType || "unknown")}</span>
        </div>

        <div class="target-section">
          <p class="target-section-title">Endpoints</p>
          <div class="endpoint-list">
            ${endpointRows || `<p class="cluster-empty">No endpoints available.</p>`}
          </div>
        </div>

        <div class="target-section">
          <p class="target-section-title">Cluster Resources</p>
          <div class="resource-grid">
            ${resourceCards || `<p class="cluster-empty">No cluster resources attached yet.</p>`}
          </div>
        </div>

        <div class="target-section">
          <p class="target-section-title">Pulsar Config</p>
          ${renderPulsarConfig(pulsarConfig)}
        </div>
      </section>
    `;
  }).join("");
}

function renderLatencyLadder(target, summary) {
  const max = Math.max(
    Number(summary?.max || 0),
    Number(summary?.p99 || 0),
    Number(summary?.p95 || 0),
    Number(summary?.p50 || 0),
    Number(summary?.avg || 0),
    1
  );
  const rows = [
    ["Avg", summary?.avg || 0],
    ["P50", summary?.p50 || 0],
    ["P95", summary?.p95 || 0],
    ["P99", summary?.p99 || 0],
    ["Max", summary?.max || 0]
  ];

  target.innerHTML = rows.map(([label, value]) => {
    const width = Math.max(4, Math.round((Number(value) / max) * 100));
    return `
      <div class="latency-row">
        <span class="latency-key">${label}</span>
        <div class="latency-bar">
          <div class="latency-bar-fill" style="width:${width}%"></div>
        </div>
        <span class="latency-value">${formatDecimal(Number(value))} ms</span>
      </div>
    `;
  }).join("");
}

function renderFlowStrip(performanceSummary) {
  const targetRate = Number(performanceSummary.targetMsgRate || 0);
  const publishRate = Number(performanceSummary.publishRate || 0);
  const consumeRate = Number(performanceSummary.consumeRate || 0);
  const publishUtilization = targetRate > 0 ? Math.min(100, (publishRate / targetRate) * 100) : 0;

  proofFlowEl.innerHTML = `
    <div class="flow-row">
      <div class="flow-row-header">
        <span class="flow-row-label">Publish Throughput</span>
        <span class="flow-row-value">${formatDecimal(publishRate)}</span>
      </div>
      <p class="flow-row-meta">${formatDecimal(publishUtilization)}% of target ${formatNumber(targetRate)} msg/s</p>
    </div>
    <div class="flow-row">
      <div class="flow-row-header">
        <span class="flow-row-label">Consume Throughput</span>
        <span class="flow-row-value">${formatDecimal(consumeRate)}</span>
      </div>
      <p class="flow-row-meta">${formatNumber(performanceSummary.consumedMessages || 0)} messages received so far</p>
    </div>
    <div class="flow-row">
      <div class="flow-row-header">
        <span class="flow-row-label">Backlog</span>
        <span class="flow-row-value">${formatNumber(performanceSummary.backlogMessages || 0)}</span>
      </div>
      <p class="flow-row-meta">${formatDecimal(performanceSummary.publishErrorRate || 0)} producer errors per second</p>
    </div>
  `;
}

function buildLinePath(points, xAccessor, yAccessor, width, height, maxX, maxY) {
  if (points.length === 0) {
    return "";
  }
  return points.map((point, index) => {
    const x = maxX <= 0 ? 0 : (xAccessor(point) / maxX) * width;
    const y = height - (maxY <= 0 ? 0 : (yAccessor(point) / maxY) * height);
    return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
  }).join(" ");
}

function renderChart(target, series, ySuffix) {
  if (!Array.isArray(series.points) || series.points.length < 2) {
    target.innerHTML = `<div class="chart-empty">Need at least two checkpoints before drawing a trend.</div>`;
    return;
  }

  const width = 640;
  const height = 180;
  const maxX = Math.max(...series.points.map((point) => point.elapsedSeconds || 0), 1);
  const maxY = Math.max(
    ...series.lines.flatMap((line) => series.points.map((point) => Number(line.accessor(point) || 0))),
    1
  );
  const gridLines = [0.25, 0.5, 0.75].map((ratio) => {
    const y = (height * ratio).toFixed(2);
    return `<line class="chart-grid-line" x1="0" y1="${y}" x2="${width}" y2="${y}"></line>`;
  }).join("");
  const paths = series.lines.map((line) => {
    const d = buildLinePath(series.points, (point) => point.elapsedSeconds || 0, line.accessor, width, height, maxX, maxY);
    return `<path class="chart-series" d="${d}" style="stroke:${line.color}"></path>`;
  }).join("");
  const legend = series.lines.map((line) => `
    <span class="chart-legend-item">
      <span class="chart-legend-swatch" style="background:${line.color}"></span>
      <span>${line.label}</span>
    </span>
  `).join("");

  target.innerHTML = `
    <div class="chart-shell">
      <div class="chart-legend">${legend}</div>
      <svg class="chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">
        ${gridLines}
        <line class="chart-axis" x1="0" y1="${height}" x2="${width}" y2="${height}"></line>
        ${paths}
      </svg>
      <div class="chart-axis-labels">
        <span>0s</span>
        <span>${formatDecimal(maxY)} ${ySuffix}</span>
        <span>${formatNumber(maxX)}s</span>
      </div>
    </div>
  `;
}

function renderProofList() {
  if (proofs.length === 0) {
    proofListEl.innerHTML = `
      <div class="proof-item">
        <p class="proof-item-title">No active proofs</p>
        <p class="proof-item-meta">Start a proof and refresh this page.</p>
      </div>
    `;
    return;
  }

  proofListEl.innerHTML = proofs.map((proof) => {
    const activeClass = proof.id === selectedProofId ? " active" : "";
    return `
      <button class="proof-item${activeClass}" data-proof-id="${proof.id}" type="button">
        <p class="proof-item-title">${proof.name || proof.id}</p>
        <p class="proof-item-meta">${proof.driver || "unknown driver"} · ${proof.topic || "no topic"}</p>
      </button>
    `;
  }).join("");

  proofListEl.querySelectorAll("[data-proof-id]").forEach((button) => {
    button.addEventListener("click", () => {
      setSelectedProofId(button.dataset.proofId);
      renderProofList();
      void loadProofDetails(selectedProofId);
    });
  });
}

async function loadProofs() {
  const response = await fetch("/proofs");
  if (!response.ok) {
    throw new Error(`Failed to load proofs: ${response.status}`);
  }
  proofs = await response.json();
  if (!selectedProofId && proofs.length > 0) {
    setSelectedProofId(proofs[0].id);
  }
  renderProofList();
  if (selectedProofId) {
    await loadProofDetails(selectedProofId);
  } else {
    showEmptyState("No proof selected", "The coordinator is reachable, but there are no active proofs yet.");
  }
}

function showEmptyState(title, subtitle) {
  heroTitleEl.textContent = title;
  heroSubtitleEl.textContent = subtitle;
  heroStatusEl.textContent = "waiting";
  heroStatusEl.className = "status-badge";
  heroProgressValueEl.textContent = "0s left";
  heroProgressDetailEl.textContent = "Elapsed 0s · Remaining 0s · Total 0s";
  heroProgressFillEl.style.width = "0%";
  emptyStateEl.classList.remove("hidden");
  detailsViewEl.classList.add("hidden");
}

async function loadProofDetails(proofId) {
  if (!proofId) {
    showEmptyState("No proof selected", "Choose a proof from the sidebar to inspect details.");
    return;
  }

  const response = await fetch(`/proofs/${proofId}/report`);
  if (!response.ok) {
    showEmptyState("Proof not found", `The proof ${proofId} is no longer available from the coordinator.`);
    return;
  }

  const data = await response.json();
  const proof = data.proof || {};
  const summary = data.summary || {};
  const checkpointSummary = data.checkpointSummary || {};
  const performanceSummary = data.performanceSummary || {};
  const timeSeries = Array.isArray(data.timeSeries) ? data.timeSeries : [];
  const resultStatus = data.resultStatus || data.status || "unknown";
  const executionStatus = data.status || "unknown";
  const elapsedSeconds = Number(performanceSummary.elapsedSeconds || 0);
  const plannedDurationSeconds = Number(performanceSummary.plannedDurationSeconds || proof.duration || 0);
  const remainingSeconds = Number(
    performanceSummary.remainingSeconds ?? Math.max(0, plannedDurationSeconds - elapsedSeconds)
  );
  const remainingPercent = plannedDurationSeconds > 0
    ? Math.max(0, Math.min(100, (remainingSeconds / plannedDurationSeconds) * 100))
    : 0;
  const progressPercent = plannedDurationSeconds > 0
    ? Math.max(0, Math.min(100, (elapsedSeconds / plannedDurationSeconds) * 100))
    : Math.max(0, Math.min(100, Number(performanceSummary.progressPercent || 0)));

  heroTitleEl.textContent = proof.name || proof.id || proofId;
  heroSubtitleEl.textContent = [
    `${executionStatus} · ${data.resultReason || "result pending"}`,
    proof.driver || "unknown driver",
    proof.topic || "no topic",
    proof.startTime || "no start time"
  ].join(" · ");
  heroStatusEl.textContent = resultStatus || "unknown";
  heroStatusEl.className = `status-badge ${String(resultStatus || "").toLowerCase()}`.trim();
  heroProgressValueEl.textContent = `${formatDecimal(progressPercent)}%`;
  heroProgressDetailEl.textContent =
    `Elapsed ${formatDuration(elapsedSeconds)} · Remaining ${formatDuration(remainingSeconds)} · `
    + `Total ${formatDuration(plannedDurationSeconds)}`;
  heroProgressFillEl.style.width = `${progressPercent}%`;

  metricEls.verified.textContent = formatNumber(summary.verified || 0);
  metricEls.missed.textContent = formatNumber(summary.missed || 0);
  metricEls.outOfOrders.textContent = formatNumber(summary.outOfOrders || 0);
  metricEls.duplicates.textContent = formatNumber(summary.duplicates || 0);
  metricEls.errors.textContent = formatNumber(summary.errors || 0);
  metricEls.timeouts.textContent = formatNumber(summary.timeouts || 0);
  performanceMetricEls.publishRate.textContent = formatDecimal(performanceSummary.publishRate || 0);
  performanceMetricEls.consumeRate.textContent = formatDecimal(performanceSummary.consumeRate || 0);
  performanceMetricEls.publishErrorRate.textContent = formatDecimal(performanceSummary.publishErrorRate || 0);
  performanceMetricEls.backlog.textContent = formatNumber(performanceSummary.backlogMessages || 0);
  performanceMetricEls.publishLatencyP95.textContent = formatDecimal(performanceSummary.publishLatency?.p95 || 0);
  performanceMetricEls.publishLatencyP99.textContent = formatDecimal(performanceSummary.publishLatency?.p99 || 0);
  performanceMetricEls.endToEndLatencyP95.textContent = formatDecimal(performanceSummary.endToEndLatency?.p95 || 0);
  performanceMetricEls.endToEndLatencyP99.textContent = formatDecimal(performanceSummary.endToEndLatency?.p99 || 0);
  renderFlowStrip(performanceSummary);
  renderLatencyLadder(publishLatencyLadderEl, performanceSummary.publishLatency);
  renderLatencyLadder(e2eLatencyLadderEl, performanceSummary.endToEndLatency);
  renderChart(throughputChartEl, {
    points: timeSeries,
    lines: [
      {label: "Publish Rate", color: "#2563eb", accessor: (point) => Number(point.publishRate || 0)},
      {label: "Consume Rate", color: "#0f766e", accessor: (point) => Number(point.consumeRate || 0)}
    ]
  }, "msg/s");
  renderChart(backlogChartEl, {
    points: timeSeries,
    lines: [
      {label: "Backlog", color: "#b45309", accessor: (point) => Number(point.backlogMessages || 0)}
    ]
  }, "messages");
  renderChart(publishLatencyChartEl, {
    points: timeSeries,
    lines: [
      {label: "P95", color: "#f97316", accessor: (point) => Number(point.publishLatencyP95 || 0)},
      {label: "P99", color: "#7c3aed", accessor: (point) => Number(point.publishLatencyP99 || 0)}
    ]
  }, "ms");
  renderChart(e2eLatencyChartEl, {
    points: timeSeries,
    lines: [
      {label: "P95", color: "#dc2626", accessor: (point) => Number(point.endToEndLatencyP95 || 0)},
      {label: "P99", color: "#0f766e", accessor: (point) => Number(point.endToEndLatencyP99 || 0)}
    ]
  }, "ms");

  renderKeyValueList(proofConfigEl, [
    ["Proof ID", proof.id],
    ["Driver", proof.driver],
    ["Drivers", proof.drivers],
    ["Features", proof.features],
    ["Topic", proof.topic],
    ["Partitions", proof.partitions],
    ["Producers", proof.producers],
    ["Consumers", proof.consumers],
    ["Message Rate", proof.msgRate],
    ["Keys", proof.keys],
    ["Checkpoint Interval", proof.checkPointInterval],
    ["Timeout", proof.timeout],
    ["Duration", proof.duration],
    ["Start Time", proof.startTime],
    ["Description", proof.description],
    ["Webhook Config", proof.webhookConfig],
    ["Pulsar Config", proof.pulsar]
  ]);

  renderKeyValueList(proofCheckpointsEl, [
    ["Current In Check Keys", checkpointSummary.inCheckKeys],
    ["Latest Producer Keys", checkpointSummary.latestProducerKeys],
    ["Latest Consumer Keys", checkpointSummary.latestConsumerKeys],
    ["Last Verified Producer Keys", checkpointSummary.verifiedProducerKeys],
    ["Last Verified Consumer Keys", checkpointSummary.verifiedConsumerKeys],
    ["Last Failed Producer Keys", checkpointSummary.failedProducerKeys],
    ["Last Failed Consumer Keys", checkpointSummary.failedConsumerKeys]
  ]);

  renderClusterTargets(data.clusterTargets || []);

  renderKeyValueList(proofPerformanceEl, [
    ["Published Messages", performanceSummary.publishedMessages],
    ["Publish Attempts", performanceSummary.publishAttempts],
    ["Publish Errors", performanceSummary.publishErrors],
    ["Consumed Messages", performanceSummary.consumedMessages],
    ["Verified Messages", performanceSummary.verifiedMessages],
    ["Backlog Messages", performanceSummary.backlogMessages]
  ]);

  proofJsonLinkEl.href = `/proofs/${proofId}/report`;

  emptyStateEl.classList.add("hidden");
  detailsViewEl.classList.remove("hidden");
}

async function refresh() {
  refreshButton.disabled = true;
  refreshButton.textContent = "Refreshing...";
  try {
    await loadProofs();
  } catch (error) {
    console.error(error);
    showEmptyState("Failed to load proofs", error.message);
  } finally {
    refreshButton.disabled = false;
    refreshButton.textContent = "Refresh";
  }
}

refreshButton.addEventListener("click", () => {
  void refresh();
});

void refresh();
