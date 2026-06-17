/// <reference types="@fastly/js-compute" />

import { createFanoutHandoff } from "fastly:fanout";
import { env } from "fastly:env";
import { KVStore } from "fastly:kv-store";
import { SecretStore } from "fastly:secret-store";

class EventBus {
  constructor() {
    this.subscribers = new Map();
  }

  get count() {
    return this.subscribers.size;
  }

  subscribe(id, writer) {
    this.subscribers.set(id, writer);
  }

  unsubscribe(id) {
    this.subscribers.delete(id);
  }

  publish(event) {
    const startTime = performance.now();
    const sseFrame = `event: notification\ndata: ${JSON.stringify(event)}\n\n`;
    const payloadBytes = new TextEncoder().encode(sseFrame).length;
    let delivered = 0;

    for (const [id, writer] of this.subscribers) {
      try {
        writer.write(new TextEncoder().encode(sseFrame));
        delivered++;
      } catch (e) {
        this.subscribers.delete(id);
      }
    }

    return {
      subscribers: delivered,
      payloadBytes,
      broadcastMs: parseFloat((performance.now() - startTime).toFixed(3)),
    };
  }
}

const eventBus = new EventBus();
let subscriberIdCounter = 0;

const PUBLISH_TOKEN = "demo-publish-token-fastly-fanout";

const SECURITY_HEADERS = {
  "X-Content-Type-Options": "nosniff",
  "X-Frame-Options": "DENY",
};

function secureResponse(body, init = {}) {
  const headers = new Headers(init.headers || {});
  for (const [k, v] of Object.entries(SECURITY_HEADERS)) {
    headers.set(k, v);
  }
  return new Response(body, { ...init, headers });
}

function sanitizeEvent(raw) {
  const parsed = JSON.parse(JSON.stringify(raw));

  if (!parsed.type || !parsed.payload) {
    throw new Error("Event must have 'type' and 'payload' fields");
  }

  const clean = JSON.parse(
    JSON.stringify(parsed).replace(/<script[\s\S]*?<\/script>/gi, "")
  );

  return clean;
}

const KV_STORE_NAME = "notification_history";
const KV_MAX_HISTORY = 20;
const KV_HISTORY_KEY = "recent-events";

function getKVStore() {
  try {
    return new KVStore(KV_STORE_NAME);
  } catch {
    return null;
  }
}

async function storeEvent(event) {
  const store = getKVStore();
  if (!store) return;
  try {
    let events = [];
    const existing = await store.get(KV_HISTORY_KEY);
    if (existing) {
      try { events = await existing.json(); } catch {}
    }
    events.unshift(event);
    if (events.length > KV_MAX_HISTORY) events = events.slice(0, KV_MAX_HISTORY);
    await store.put(KV_HISTORY_KEY, JSON.stringify(events), { ttl: 3600 });
  } catch {}
}

async function getRecentEvents() {
  const store = getKVStore();
  if (!store) return [];
  try {
    const entry = await store.get(KV_HISTORY_KEY);
    if (!entry) return [];
    const events = await entry.json();
    return Array.isArray(events) ? events.slice(0, KV_MAX_HISTORY) : [];
  } catch {
    return [];
  }
}

async function fanoutPublish(event) {
  const sseFrame = `event: notification\ndata: ${JSON.stringify(event)}\n\n`;
  const serviceId = env("FASTLY_SERVICE_ID");
  if (!serviceId) throw new Error("No service ID");

  const secrets = new SecretStore("fastly_fanout");
  const tokenEntry = await secrets.get("fastly-api-token");
  if (!tokenEntry) throw new Error("No API token");
  const apiToken = tokenEntry.plaintext();
  if (!apiToken) throw new Error("Empty API token");

  const response = await fetch(
    `https://api.fastly.com/service/${serviceId}/publish/`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Fastly-Key": apiToken,
      },
      body: JSON.stringify({
        items: [{
          channel: "notifications",
          formats: { "http-stream": { content: sseFrame } },
        }],
      }),
      backend: "fanout_publish",
    }
  );

  if (!response.ok) throw new Error("Fanout publish failed");

  return {
    subscribers: "edge",
    payloadBytes: new TextEncoder().encode(sseFrame).length,
    broadcastMs: 0,
  };
}

addEventListener("fetch", (event) => event.respondWith(handleRequest(event)));

async function handleRequest(event) {
  const url = new URL(event.request.url);

  switch (url.pathname) {
    case "/":
      return handleIndex();
    case "/subscribe":
      return handleSubscribe(event);
    case "/publish":
      return handlePublish(event);
    case "/demo-publish":
      return handleDemoPublish(event);
    default:
      return secureResponse("Not Found", { status: 404 });
  }
}

async function handleSubscribe(event) {
  const req = event.request;
  const gripSig = req.headers.get("Grip-Sig");

  // Production: Fanout-relayed request (has Grip-Sig)
  if (gripSig) {
    const history = await getRecentEvents();
    const historyFrames = history.reverse().map((evt) =>
      `event: notification\ndata: ${JSON.stringify(evt)}\n\n`
    ).join("");
    const init = `event: connected\ndata: ${JSON.stringify({ mode: "fanout" })}\n\n`;

    return secureResponse(init + historyFrames, {
      status: 200,
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Grip-Hold": "stream",
        "Grip-Channel": "notifications",
        "Grip-Keep-Alive": "\\n; format=cstring; timeout=20",
      },
    });
  }

  // Production: Fanout handoff (skip in Viceroy — it can't handle it)
  try {
    const hostname = env("FASTLY_HOSTNAME");
    if (hostname && hostname !== "localhost") {
      return createFanoutHandoff(req, "self");
    }
  } catch {
    // env() or handoff failed — fall through to EventBus
  }

  // Local: EventBus fallback
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const id = ++subscriberIdCounter;
  eventBus.subscribe(id, writer);

  const init = `event: connected\ndata: ${JSON.stringify({ subscriberId: id, mode: "local" })}\n\n`;
  writer.write(new TextEncoder().encode(init));

  const history = await getRecentEvents();
  for (const evt of history.reverse()) {
    writer.write(new TextEncoder().encode(`event: notification\ndata: ${JSON.stringify(evt)}\n\n`));
  }

  return secureResponse(readable, {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
    },
  });
}

async function handlePublish(event) {
  if (event.request.method !== "POST") {
    return secureResponse("Method Not Allowed", { status: 405 });
  }

  const token = event.request.headers.get("X-Publish-Token");
  if (!token) {
    return secureResponse(JSON.stringify({ error: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }

  let authorized = false;
  try {
    const secrets = new SecretStore("fastly_fanout");
    const entry = await secrets.get("publish-token");
    if (entry && entry.plaintext() === token) authorized = true;
  } catch {}
  if (!authorized && token === PUBLISH_TOKEN) authorized = true;
  if (!authorized) {
    return secureResponse(JSON.stringify({ error: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }

  let body;
  try {
    body = await event.request.json();
  } catch {
    return secureResponse(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  let sanitized;
  try {
    sanitized = sanitizeEvent(body);
  } catch {
    return secureResponse(JSON.stringify({ error: "Invalid event format" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  const storePromise = storeEvent(sanitized);

  let stats;
  let mode;
  try {
    stats = await fanoutPublish(sanitized);
    mode = "fanout";
  } catch {
    stats = eventBus.publish(sanitized);
    mode = "local";
  }

  await storePromise.catch(() => {});

  return secureResponse(
    JSON.stringify({
      success: true,
      event: sanitized,
      broadcast: { ...stats, mode },
    }),
    {
      status: 200,
      headers: {
        "Content-Type": "application/json",
      },
    }
  );
}

const DEMO_BUDGET_PER_HOUR = 20;
const DEMO_GLOBAL_PER_MINUTE = 10;

async function handleDemoPublish(event) {
  if (event.request.method !== "POST") {
    return secureResponse("Method Not Allowed", { status: 405 });
  }

  const clientIp = event.client?.address || event.request.headers.get("fastly-client-ip") || "unknown";
  const ipHash = Array.from(new TextEncoder().encode(clientIp)).reduce((h, b) => ((h << 5) - h + b) | 0, 0).toString(36);

  const store = getKVStore();
  if (store) {
    try {
      const hourKey = `demo:${ipHash}:${Math.floor(Date.now() / 3600000)}`;
      const existing = await store.get(hourKey);
      const count = existing ? parseInt(await existing.text(), 10) || 0 : 0;
      if (count >= DEMO_BUDGET_PER_HOUR) {
        return secureResponse(JSON.stringify({ error: "Demo budget reached. Events will continue automatically.", budgetExhausted: true }), {
          status: 429,
          headers: { "Content-Type": "application/json" },
        });
      }

      const minuteKey = `rate:${Math.floor(Date.now() / 60000)}`;
      const rateEntry = await store.get(minuteKey);
      const rateCount = rateEntry ? parseInt(await rateEntry.text(), 10) || 0 : 0;
      if (rateCount >= DEMO_GLOBAL_PER_MINUTE) {
        return secureResponse(JSON.stringify({ error: "Rate limit exceeded. Try again shortly." }), {
          status: 429,
          headers: { "Content-Type": "application/json" },
        });
      }

      await store.put(hourKey, String(count + 1), { ttl: 3600 });
      await store.put(minuteKey, String(rateCount + 1), { ttl: 120 });
    } catch {}
  }

  let body;
  try {
    body = await event.request.json();
  } catch {
    return secureResponse(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  let sanitized;
  try {
    sanitized = sanitizeEvent(body);
  } catch {
    return secureResponse(JSON.stringify({ error: "Invalid event format" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  const storePromise = storeEvent(sanitized);

  let stats;
  let mode;
  try {
    stats = await fanoutPublish(sanitized);
    mode = "fanout";
  } catch {
    stats = eventBus.publish(sanitized);
    mode = "local";
  }

  await storePromise.catch(() => {});

  return secureResponse(
    JSON.stringify({
      success: true,
      event: sanitized,
      broadcast: { ...stats, mode },
    }),
    {
      status: 200,
      headers: { "Content-Type": "application/json" },
    }
  );
}

function handleIndex() {
  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Fastly Fanout — Real-Time Notifications Demo</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }

    :root {
      --bg: #f8fafc;
      --surface: #ffffff;
      --primary: #6366f1;
      --primary-hover: #4f46e5;
      --primary-light: #eef2ff;
      --secondary: #475569;
      --text: #0f172a;
      --text-secondary: #64748b;
      --text-muted: #94a3b8;
      --success: #22c55e;
      --alert: #ef4444;
      --alert-hover: #dc2626;
      --alert-light: #fef2f2;
      --border: #e2e8f0;
      --border-light: #f1f5f9;
      --shadow-sm: 0 1px 2px rgba(0,0,0,0.05);
      --shadow-md: 0 4px 6px -1px rgba(0,0,0,0.07), 0 2px 4px -2px rgba(0,0,0,0.05);
      --shadow-lg: 0 10px 15px -3px rgba(0,0,0,0.07), 0 4px 6px -4px rgba(0,0,0,0.05);
      --radius: 12px;
    }

    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
      height: 100vh;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }

    .top-bar {
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 14px 24px;
      display: flex;
      align-items: center;
      gap: 14px;
      flex-shrink: 0;
    }

    .top-bar h1 {
      font-size: 16px;
      font-weight: 700;
      color: var(--text);
    }

    .top-bar .badge {
      background: var(--primary);
      color: #fff;
      font-size: 10px;
      font-weight: 600;
      padding: 3px 10px;
      border-radius: 12px;
      letter-spacing: 0.5px;
    }

    .main {
      display: grid;
      grid-template-columns: 1fr 1.2fr 1fr;
      gap: 16px;
      padding: 16px;
      flex: 7;
      min-height: 0;
    }

    .panel {
      background: var(--surface);
      border-radius: var(--radius);
      border: 1px solid var(--border);
      padding: 20px;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      box-shadow: var(--shadow-sm);
      transition: box-shadow 0.2s ease;
    }

    .panel:hover {
      box-shadow: var(--shadow-md);
    }

    .panel-title {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1.5px;
      color: var(--text-secondary);
      margin-bottom: 16px;
    }

    /* Publisher Panel */
    .publisher-actions {
      display: flex;
      flex-direction: column;
      gap: 12px;
      flex: 1;
      justify-content: center;
    }

    .publish-btn {
      padding: 14px 20px;
      border: none;
      border-radius: var(--radius);
      font-size: 14px;
      font-weight: 600;
      cursor: pointer;
      transition: all 0.15s ease;
    }

    .publish-btn:active {
      transform: scale(0.97);
    }

    .btn-breaking {
      background: var(--alert);
      color: #fff;
      box-shadow: var(--shadow-sm);
    }

    .btn-breaking:hover {
      background: var(--alert-hover);
      box-shadow: var(--shadow-md);
    }

    .btn-score {
      background: var(--primary);
      color: #fff;
      box-shadow: var(--shadow-sm);
    }

    .btn-score:hover:not(:disabled) {
      background: var(--primary-hover);
      box-shadow: var(--shadow-md);
    }

    .publish-btn:disabled {
      opacity: 0.4;
      cursor: not-allowed;
      transform: none;
    }

    .budget-notice {
      font-size: 12px;
      color: var(--text-muted);
      text-align: center;
      margin-top: 8px;
      line-height: 1.4;
    }

    .auto-badge {
      display: inline-block;
      font-size: 10px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 1px;
      padding: 3px 8px;
      border-radius: 4px;
      background: var(--primary-light);
      color: var(--primary);
      margin-top: 8px;
    }

    /* Phone Mockup */
    .phone-container {
      display: flex;
      align-items: center;
      justify-content: center;
      flex: 1;
    }

    .phone {
      width: 280px;
      height: 100%;
      max-height: 480px;
      background: var(--surface);
      border-radius: 32px;
      border: 3px solid var(--text);
      box-shadow: var(--shadow-lg);
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }

    .phone-status-bar {
      background: var(--text);
      color: #fff;
      font-size: 11px;
      padding: 8px 16px 6px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .status-dot {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: var(--success);
      display: inline-block;
      margin-right: 6px;
    }

    .status-dot.disconnected {
      background: var(--alert);
      animation: blink 1s infinite;
    }

    @keyframes blink {
      50% { opacity: 0.3; }
    }

    .phone-content {
      flex: 1;
      display: flex;
      flex-direction: column;
    }

    .news-section {
      flex: 1;
      padding: 16px;
      display: flex;
      flex-direction: column;
      justify-content: center;
      border-bottom: 1px solid var(--border);
      position: relative;
      overflow: hidden;
    }

    .news-label {
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: var(--alert);
      margin-bottom: 8px;
    }

    .news-banner {
      background: var(--surface);
      border-left: 3px solid transparent;
      padding: 10px 12px;
      border-radius: 8px;
      font-size: 13px;
      font-weight: 600;
      line-height: 1.4;
      transition: all 0.3s ease;
      opacity: 0;
      transform: translateY(-10px);
    }

    .news-banner.active {
      border-left-color: var(--alert);
      background: var(--alert-light);
      opacity: 1;
      transform: translateY(0);
    }

    .news-banner .time {
      font-size: 10px;
      font-weight: 400;
      color: var(--text-muted);
      margin-top: 4px;
    }

    .score-section {
      flex: 1;
      padding: 16px;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      background: var(--border-light);
    }

    .score-label {
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: var(--primary);
      margin-bottom: 12px;
    }

    .scoreboard {
      display: flex;
      align-items: center;
      gap: 16px;
    }

    .team {
      text-align: center;
    }

    .team-name {
      font-size: 12px;
      font-weight: 600;
      color: var(--text-secondary);
      margin-bottom: 4px;
    }

    .team-score {
      font-size: 36px;
      font-weight: 800;
      color: var(--text);
      transition: transform 0.2s ease, color 0.2s ease;
    }

    .team-score.bumped {
      transform: scale(1.3);
      color: var(--primary);
    }

    .score-divider {
      font-size: 20px;
      color: var(--text-muted);
      font-weight: 300;
    }

    /* X-Ray Dashboard */
    .dashboard-stats {
      display: flex;
      flex-direction: column;
      gap: 8px;
      margin-bottom: 16px;
    }

    .stat-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 10px 12px;
      border-radius: 8px;
      background: var(--border-light);
      border: 1px solid var(--border);
    }

    .stat-label {
      font-size: 12px;
      color: var(--text-secondary);
      font-weight: 500;
    }

    .stat-value {
      font-size: 14px;
      font-weight: 700;
      font-variant-numeric: tabular-nums;
      color: var(--primary);
    }

    .stat-note {
      font-size: 10px;
      color: var(--text-muted);
      font-style: italic;
      line-height: 1.4;
      padding: 8px 0;
    }

    .event-log-title {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1.5px;
      color: var(--text-secondary);
      margin-bottom: 8px;
    }

    .event-log {
      flex: 1;
      overflow-y: auto;
      font-size: 11px;
      font-family: "SF Mono", "Fira Code", monospace;
      min-height: 0;
    }

    .log-entry {
      padding: 5px 6px;
      margin-bottom: 2px;
      border-radius: 6px;
      background: var(--border-light);
      display: flex;
      gap: 8px;
      transition: background 0.15s;
    }

    .log-entry:hover {
      background: var(--primary-light);
    }

    .log-time {
      color: var(--text-muted);
      flex-shrink: 0;
    }

    .log-type {
      color: var(--primary);
      font-weight: 700;
      flex-shrink: 0;
    }

    .log-detail {
      color: var(--text-secondary);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    /* Architecture Diagram */
    .arch-strip {
      flex: 3;
      background: var(--surface);
      border-top: 1px solid var(--border);
      padding: 16px 24px;
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      min-height: 0;
    }

    .arch-title {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1.5px;
      color: var(--text-secondary);
      margin-bottom: 12px;
    }

    .arch-flow {
      display: flex;
      align-items: center;
      gap: 0;
      position: relative;
      width: 100%;
      max-width: 700px;
      justify-content: center;
    }

    .arch-node {
      background: var(--surface);
      border: 2px solid var(--border);
      border-radius: 8px;
      padding: 10px 16px;
      font-size: 12px;
      font-weight: 600;
      text-align: center;
      transition: box-shadow 0.3s, border-color 0.3s, transform 0.3s;
      z-index: 1;
      white-space: nowrap;
      color: var(--text);
    }

    .arch-node.glow {
      border-color: var(--primary);
      box-shadow: 0 0 0 3px rgba(99,102,241,0.15);
      transform: scale(1.05);
    }

    .arch-connector {
      flex: 1;
      height: 2px;
      background: var(--border);
      position: relative;
      min-width: 40px;
    }

    .arch-connector-label {
      position: absolute;
      top: -16px;
      left: 50%;
      transform: translateX(-50%);
      font-size: 9px;
      color: var(--text-muted);
      white-space: nowrap;
      font-weight: 500;
    }

    .arch-dot {
      position: absolute;
      width: 8px;
      height: 8px;
      background: var(--primary);
      border-radius: 50%;
      top: -3px;
      left: 0;
      opacity: 0;
      z-index: 2;
      box-shadow: 0 0 8px rgba(99,102,241,0.4);
    }

    .arch-dot.animate {
      opacity: 1;
      animation: travel 1.5s ease-in-out forwards;
    }

    @keyframes travel {
      0% { left: 0%; opacity: 1; }
      100% { left: 100%; opacity: 0; }
    }

    .arch-caption {
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 12px;
      text-align: center;
      font-style: italic;
    }
  </style>
</head>
<body>
  <div class="top-bar">
    <h1>Fastly Fanout</h1>
    <span class="badge">LIVE DEMO</span>
  </div>

  <div class="main">
    <!-- Publisher Panel -->
    <div class="panel">
      <div class="panel-title">Publisher</div>
      <div class="publisher-actions">
        <button class="publish-btn btn-breaking" id="breaking-btn">
          Send Breaking News
        </button>
        <button class="publish-btn btn-score" id="score-btn">
          Increment Score +1
        </button>
      </div>
      <div class="budget-notice" id="budget-notice"></div>
    </div>

    <!-- Phone Mockup -->
    <div class="panel" style="padding: 20px; display: flex; align-items: center; justify-content: center;">
      <div class="phone">
        <div class="phone-status-bar">
          <span><span class="status-dot" id="connection-dot"></span><span id="connection-status">Connected</span></span>
          <span>Fastly Edge</span>
        </div>
        <div class="phone-content">
          <div class="news-section">
            <div class="news-label">Breaking News</div>
            <div class="news-banner" id="news-banner">
              <div id="news-headline">Waiting for events...</div>
              <div class="time" id="news-time"></div>
            </div>
          </div>
          <div class="score-section">
            <div class="score-label">Live Score</div>
            <div class="scoreboard">
              <div class="team">
                <div class="team-name">HOME</div>
                <div class="team-score" id="home-score">0</div>
              </div>
              <div class="score-divider">:</div>
              <div class="team">
                <div class="team-name">AWAY</div>
                <div class="team-score" id="away-score">0</div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- X-Ray Dashboard -->
    <div class="panel">
      <div class="panel-title">X-Ray Dashboard</div>
      <div class="dashboard-stats">
        <div class="stat-row">
          <span class="stat-label">Active Connections</span>
          <span class="stat-value" id="connections-count">0</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">Last Payload</span>
          <span class="stat-value" id="payload-size">— bytes</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">Broadcast Latency</span>
          <span class="stat-value" id="broadcast-latency">— ms</span>
        </div>
        <div class="stat-note">
          In production, Fastly Fanout achieves &lt;1ms edge broadcast across 90+ global PoPs
        </div>
      </div>
      <div class="event-log-title">Event Log</div>
      <div class="event-log" id="event-log"></div>
    </div>
  </div>

  <!-- Architecture Diagram -->
  <div class="arch-strip">
    <div class="arch-title">Live Architecture Flow</div>
    <div class="arch-flow">
      <div class="arch-node" id="arch-publisher">Publisher</div>
      <div class="arch-connector">
        <span class="arch-connector-label">POST /publish</span>
        <div class="arch-dot" id="arch-dot-1"></div>
      </div>
      <div class="arch-node" id="arch-auth">Auth + Sanitize</div>
      <div class="arch-connector">
        <span class="arch-connector-label" id="arch-transport-label">EventBus</span>
        <div class="arch-dot" id="arch-dot-2"></div>
      </div>
      <div class="arch-node" id="eventbus-node"><span id="transport-node-label">EventBus</span></div>
      <div class="arch-connector">
        <span class="arch-connector-label">SSE stream</span>
        <div class="arch-dot" id="arch-dot-3"></div>
      </div>
      <div class="arch-node" id="arch-client">Client</div>
    </div>
    <div class="arch-caption" id="arch-caption">
      Detecting runtime mode...
    </div>
  </div>

  <script>
    let homeScore = 0;
    let awayScore = 0;

    const breakingHeadlines = [
      "Major trade deal announced between division rivals",
      "Star player returns from injury ahead of schedule",
      "Championship venue confirmed for next season",
      "Record-breaking transfer fee accepted",
      "Coach fired after surprising loss in playoffs",
      "League announces new expansion team",
    ];
    let headlineIndex = 0;

    // --- SSE Connection ---
    function connectSSE() {
      const evtSource = new EventSource("/subscribe");
      const dot = document.getElementById("connection-dot");
      const status = document.getElementById("connection-status");

      evtSource.addEventListener("connected", (e) => {
        const data = JSON.parse(e.data);
        window._connectionMode = data.mode;
        dot.classList.remove("disconnected");
        status.textContent = "Connected";

        const transportLabel = document.getElementById("arch-transport-label");
        const transportNode = document.getElementById("transport-node-label");
        const caption = document.getElementById("arch-caption");

        if (data.mode === "fanout") {
          transportLabel.textContent = "Fanout API";
          transportNode.textContent = "Fanout";
          caption.textContent = "Connected via Fastly Fanout — connections held at the edge across 90+ global PoPs";
        } else {
          transportLabel.textContent = "EventBus";
          transportNode.textContent = "EventBus";
          caption.textContent = "Running locally via EventBus — in production, Fastly Fanout holds connections at the edge";
        }
      });

      evtSource.addEventListener("notification", (e) => {
        const event = JSON.parse(e.data);
        handleEvent(event);
      });

      evtSource.onerror = () => {
        dot.classList.add("disconnected");
        status.textContent = "Reconnecting...";
      };

      evtSource.onopen = () => {
        dot.classList.remove("disconnected");
        status.textContent = "Connected";
      };
    }

    // --- Event Handlers ---
    function handleEvent(event) {
      if (event.type === "breaking-news") {
        showBreakingNews(event.payload);
      } else if (event.type === "score-update") {
        updateScore(event.payload);
      }
      addLogEntry(event);
    }

    function showBreakingNews(payload) {
      const banner = document.getElementById("news-banner");
      const headline = document.getElementById("news-headline");
      const time = document.getElementById("news-time");

      banner.classList.remove("active");

      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          headline.textContent = payload.headline;
          time.textContent = new Date(payload.timestamp).toLocaleTimeString();
          banner.classList.add("active");
        });
      });
    }

    function updateScore(payload) {
      const homeEl = document.getElementById("home-score");
      const awayEl = document.getElementById("away-score");

      homeEl.textContent = payload.homeScore;
      awayEl.textContent = payload.awayScore;

      const changed = payload.homeScore > homeScore ? homeEl : awayEl;
      changed.classList.add("bumped");
      setTimeout(() => changed.classList.remove("bumped"), 300);

      homeScore = payload.homeScore;
      awayScore = payload.awayScore;
    }

    // --- Dashboard ---
    function updateDashboard(broadcastStats) {
      document.getElementById("connections-count").textContent =
        broadcastStats.subscribers === "edge" ? "Edge (Fanout)" : broadcastStats.subscribers;
      document.getElementById("payload-size").textContent = broadcastStats.payloadBytes + " bytes";
      document.getElementById("broadcast-latency").textContent =
        broadcastStats.mode === "fanout" ? "<1ms (edge)" : broadcastStats.broadcastMs + " ms";
    }

    function addLogEntry(event) {
      const log = document.getElementById("event-log");
      const entry = document.createElement("div");
      entry.className = "log-entry";

      const time = document.createElement("span");
      time.className = "log-time";
      time.textContent = new Date().toLocaleTimeString();

      const type = document.createElement("span");
      type.className = "log-type";
      type.textContent = event.type;

      const detail = document.createElement("span");
      detail.className = "log-detail";
      detail.textContent = event.type === "breaking-news"
        ? event.payload.headline
        : "Score: " + event.payload.homeScore + " - " + event.payload.awayScore;

      entry.appendChild(time);
      entry.appendChild(type);
      entry.appendChild(detail);
      log.prepend(entry);

      while (log.children.length > 50) {
        log.removeChild(log.lastChild);
      }
    }

    // --- Architecture Animation ---
    function animateArchDiagram() {
      const dots = [
        document.getElementById("arch-dot-1"),
        document.getElementById("arch-dot-2"),
        document.getElementById("arch-dot-3"),
      ];
      const eventbusNode = document.getElementById("eventbus-node");

      dots.forEach((dot, i) => {
        dot.classList.remove("animate");
        void dot.offsetWidth;
        setTimeout(() => dot.classList.add("animate"), i * 400);
      });

      setTimeout(() => {
        eventbusNode.classList.add("glow");
        setTimeout(() => eventbusNode.classList.remove("glow"), 600);
      }, 800);
    }

    // --- Publish Actions (server-enforced budget) ---
    const PUBLISH_COOLDOWN = 1500;
    let lastPublish = 0;
    let budgetExhausted = false;
    const breakingBtn = document.getElementById("breaking-btn");
    const scoreBtn = document.getElementById("score-btn");
    const budgetNotice = document.getElementById("budget-notice");

    function enterAutoMode() {
      if (budgetExhausted) return;
      budgetExhausted = true;
      breakingBtn.disabled = true;
      scoreBtn.disabled = true;
      budgetNotice.innerHTML = '<span class="auto-badge">Auto-Demo Active</span><br>Live events stream automatically. Open another tab to see real-time sync.';
      startAutoDemo();
    }

    async function publish(eventData) {
      if (budgetExhausted) return;
      const now = Date.now();
      if (now - lastPublish < PUBLISH_COOLDOWN) return;
      lastPublish = now;
      animateArchDiagram();

      const res = await fetch("/demo-publish", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(eventData),
      });

      const result = await res.json();

      if (res.status === 429 && result.budgetExhausted) {
        enterAutoMode();
        return;
      }

      if (result.broadcast) {
        updateDashboard(result.broadcast);
      }
      if (result.event && window._connectionMode !== "fanout") {
        handleEvent(result.event);
      }
    }

    breakingBtn.addEventListener("click", () => {
      const headline = breakingHeadlines[headlineIndex % breakingHeadlines.length];
      headlineIndex++;
      publish({
        type: "breaking-news",
        payload: {
          headline: headline,
          timestamp: Date.now(),
        },
      });
    });

    scoreBtn.addEventListener("click", () => {
      const isHome = Math.random() > 0.5;
      publish({
        type: "score-update",
        payload: {
          homeTeam: "HOME",
          awayTeam: "AWAY",
          homeScore: isHome ? homeScore + 1 : homeScore,
          awayScore: isHome ? awayScore : awayScore + 1,
          timestamp: Date.now(),
        },
      });
    });

    // --- Auto-Demo Mode ---
    let autoDemoRunning = false;
    const autoHeadlines = [
      "Injury report: star forward listed as day-to-day",
      "Overtime thriller ends with buzzer-beater three",
      "League MVP voting results announced",
      "Playoff bracket set after final regular season games",
      "Rookie sensation breaks scoring record",
      "Historic comeback: team rallies from 25-point deficit",
    ];

    function startAutoDemo() {
      if (autoDemoRunning) return;
      autoDemoRunning = true;
      let autoIndex = 0;

      setInterval(() => {
        autoIndex++;
        const isScore = autoIndex % 3 !== 0;
        let event;

        if (isScore) {
          const side = Math.random() > 0.5;
          if (side) homeScore++; else awayScore++;
          event = {
            type: "score-update",
            payload: { homeTeam: "HOME", awayTeam: "AWAY", homeScore, awayScore, timestamp: Date.now() },
          };
        } else {
          event = {
            type: "breaking-news",
            payload: { headline: autoHeadlines[autoIndex % autoHeadlines.length], timestamp: Date.now() },
          };
        }

        animateArchDiagram();
        handleEvent(event);
        updateDashboard({
          subscribers: "demo",
          payloadBytes: new TextEncoder().encode(JSON.stringify(event)).length,
          broadcastMs: (Math.random() * 0.3).toFixed(3),
          mode: "auto-demo",
        });
      }, 4000 + Math.random() * 3000);
    }

    // --- Init ---
    connectSSE();
    updateBudgetUI();
  </script>
</body>
</html>`;

  return secureResponse(html, {
    status: 200,
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      "Content-Security-Policy": "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'",
    },
  });
}
