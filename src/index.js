/// <reference types="@fastly/js-compute" />

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
    default:
      return secureResponse("Not Found", { status: 404 });
  }
}

function handleSubscribe(event) {
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const id = ++subscriberIdCounter;

  eventBus.subscribe(id, writer);

  const initMessage = `event: connected\ndata: ${JSON.stringify({ subscriberId: id })}\n\n`;
  writer.write(new TextEncoder().encode(initMessage));

  return secureResponse(readable, {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
      "Grip-Hold": "stream",
      "Grip-Channel": "notifications",
    },
  });
}

async function handlePublish(event) {
  if (event.request.method !== "POST") {
    return secureResponse("Method Not Allowed", { status: 405 });
  }

  const token = event.request.headers.get("X-Publish-Token");
  if (token !== PUBLISH_TOKEN) {
    return secureResponse(JSON.stringify({ error: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }

  let body;
  try {
    body = await event.request.json();
  } catch (e) {
    return secureResponse(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  let sanitized;
  try {
    sanitized = sanitizeEvent(body);
  } catch (e) {
    return secureResponse(JSON.stringify({ error: "Invalid event format" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  const stats = eventBus.publish(sanitized);

  return secureResponse(
    JSON.stringify({
      success: true,
      event: sanitized,
      broadcast: stats,
    }),
    {
      status: 200,
      headers: {
        "Content-Type": "application/json",
      },
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
      --fastly-red: #ff282d;
      --coral: #ff6b6b;
      --electric-violet: #7c3aed;
      --vivid-blue: #3b82f6;
      --emerald: #10b981;
      --amber: #f59e0b;
      --hot-pink: #ec4899;
      --gradient-hero: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      --gradient-fire: linear-gradient(135deg, #ff282d 0%, #ff6b6b 50%, #f59e0b 100%);
      --gradient-ocean: linear-gradient(135deg, #3b82f6 0%, #7c3aed 100%);
      --gradient-mesh: radial-gradient(at 20% 80%, rgba(124,58,237,0.08) 0%, transparent 50%),
                        radial-gradient(at 80% 20%, rgba(59,130,246,0.08) 0%, transparent 50%),
                        radial-gradient(at 50% 50%, rgba(255,40,45,0.04) 0%, transparent 60%);
      --glow-red: 0 0 20px rgba(255,40,45,0.35), 0 0 60px rgba(255,40,45,0.1);
      --glow-violet: 0 0 20px rgba(124,58,237,0.35), 0 0 60px rgba(124,58,237,0.1);
      --glow-blue: 0 0 20px rgba(59,130,246,0.35), 0 0 60px rgba(59,130,246,0.1);
    }

    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f0f2f5;
      background-image: var(--gradient-mesh);
      color: #333;
      height: 100vh;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }

    .top-bar {
      background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
      padding: 14px 24px;
      display: flex;
      align-items: center;
      gap: 14px;
      flex-shrink: 0;
      box-shadow: 0 4px 20px rgba(0,0,0,0.15);
    }

    .top-bar h1 {
      font-size: 17px;
      font-weight: 700;
      color: #fff;
      letter-spacing: 0.5px;
    }

    .top-bar .badge {
      background: var(--gradient-fire);
      color: #fff;
      font-size: 10px;
      font-weight: 700;
      padding: 3px 10px;
      border-radius: 12px;
      letter-spacing: 1px;
      animation: badge-pulse 2s ease-in-out infinite;
    }

    @keyframes badge-pulse {
      0%, 100% { box-shadow: 0 0 8px rgba(255,40,45,0.4); }
      50% { box-shadow: 0 0 16px rgba(255,40,45,0.7), 0 0 32px rgba(255,107,107,0.3); }
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
      background: rgba(255,255,255,0.85);
      backdrop-filter: blur(12px);
      border-radius: 16px;
      border: 1px solid rgba(255,255,255,0.6);
      padding: 20px;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      box-shadow: 0 4px 24px rgba(0,0,0,0.06), 0 1px 4px rgba(0,0,0,0.04);
      transition: box-shadow 0.3s ease;
    }

    .panel:hover {
      box-shadow: 0 8px 32px rgba(0,0,0,0.1), 0 2px 8px rgba(0,0,0,0.06);
    }

    .panel-title {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1.5px;
      background: var(--gradient-ocean);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      margin-bottom: 16px;
    }

    /* Publisher Panel */
    .publisher-actions {
      display: flex;
      flex-direction: column;
      gap: 14px;
      flex: 1;
      justify-content: center;
    }

    .publish-btn {
      padding: 16px 20px;
      border: none;
      border-radius: 12px;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
      transition: transform 0.15s, box-shadow 0.3s;
      position: relative;
      overflow: hidden;
    }

    .publish-btn::after {
      content: '';
      position: absolute;
      inset: 0;
      background: linear-gradient(rgba(255,255,255,0.2), transparent);
      pointer-events: none;
    }

    .publish-btn:active {
      transform: scale(0.96);
    }

    .btn-breaking {
      background: var(--gradient-fire);
      color: #fff;
      box-shadow: 0 4px 16px rgba(255, 40, 45, 0.35);
    }

    .btn-breaking:hover {
      box-shadow: var(--glow-red);
      transform: translateY(-1px);
    }

    .btn-score {
      background: var(--gradient-ocean);
      color: #fff;
      box-shadow: 0 4px 16px rgba(59,130,246,0.3);
    }

    .btn-score:hover {
      box-shadow: var(--glow-blue);
      transform: translateY(-1px);
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
      background: #fff;
      border-radius: 32px;
      border: 3px solid #1a1a2e;
      box-shadow: 0 8px 40px rgba(0,0,0,0.15), 0 0 0 1px rgba(255,255,255,0.1) inset;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }

    .phone-status-bar {
      background: linear-gradient(135deg, #1a1a2e 0%, #0f3460 100%);
      color: #fff;
      font-size: 11px;
      padding: 8px 16px 6px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .status-dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--emerald);
      display: inline-block;
      margin-right: 6px;
      box-shadow: 0 0 8px rgba(16,185,129,0.6);
    }

    .status-dot.disconnected {
      background: var(--fastly-red);
      box-shadow: 0 0 8px rgba(255,40,45,0.6);
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
      border-bottom: 1px solid #eee;
      position: relative;
      overflow: hidden;
    }

    .news-label {
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: var(--fastly-red);
      margin-bottom: 8px;
    }

    .news-banner {
      background: #fff;
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
      border-left-color: var(--fastly-red);
      background: linear-gradient(135deg, #fff5f5 0%, #fff0f6 100%);
      opacity: 1;
      transform: translateY(0);
      box-shadow: 0 2px 12px rgba(255,40,45,0.1);
    }

    .news-banner .time {
      font-size: 10px;
      font-weight: 400;
      color: #999;
      margin-top: 4px;
    }

    .score-section {
      flex: 1;
      padding: 16px;
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      background: linear-gradient(180deg, #fff 0%, #f8faff 100%);
    }

    .score-label {
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: var(--vivid-blue);
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
      color: #666;
      margin-bottom: 4px;
    }

    .team-score {
      font-size: 36px;
      font-weight: 800;
      background: var(--gradient-ocean);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      transition: transform 0.2s ease;
    }

    .team-score.bumped {
      transform: scale(1.3);
      background: var(--gradient-fire);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }

    .score-divider {
      font-size: 20px;
      color: #ccc;
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
      border-radius: 10px;
      background: linear-gradient(135deg, #f8f9ff 0%, #f0f4ff 100%);
      border: 1px solid rgba(59,130,246,0.08);
      transition: border-color 0.3s;
    }

    .stat-row:hover {
      border-color: rgba(59,130,246,0.2);
    }

    .stat-label {
      font-size: 12px;
      color: #666;
      font-weight: 500;
    }

    .stat-value {
      font-size: 14px;
      font-weight: 700;
      font-variant-numeric: tabular-nums;
      background: var(--gradient-ocean);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }

    .stat-note {
      font-size: 10px;
      color: #999;
      font-style: italic;
      line-height: 1.4;
      padding: 8px 0;
    }

    .event-log-title {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1.5px;
      background: var(--gradient-hero);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
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
      background: linear-gradient(135deg, rgba(255,40,45,0.03) 0%, rgba(124,58,237,0.03) 100%);
      display: flex;
      gap: 8px;
      transition: background 0.2s;
    }

    .log-entry:hover {
      background: linear-gradient(135deg, rgba(255,40,45,0.08) 0%, rgba(124,58,237,0.08) 100%);
    }

    .log-time {
      color: #999;
      flex-shrink: 0;
    }

    .log-type {
      background: var(--gradient-fire);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
      font-weight: 700;
      flex-shrink: 0;
    }

    .log-detail {
      color: #555;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    /* Architecture Diagram */
    .arch-strip {
      flex: 3;
      background: linear-gradient(180deg, rgba(255,255,255,0.9) 0%, rgba(240,242,255,0.95) 100%);
      border-top: 2px solid transparent;
      border-image: linear-gradient(90deg, var(--fastly-red), var(--electric-violet), var(--vivid-blue)) 1;
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
      background: var(--gradient-hero);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
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
      background: linear-gradient(135deg, #fff 0%, #f8f9ff 100%);
      border: 2px solid #e0e0e0;
      border-radius: 10px;
      padding: 10px 16px;
      font-size: 12px;
      font-weight: 700;
      text-align: center;
      transition: box-shadow 0.3s, border-color 0.3s, transform 0.3s;
      z-index: 1;
      white-space: nowrap;
      color: #333;
    }

    .arch-node:first-child {
      border-color: var(--fastly-red);
      box-shadow: 0 2px 8px rgba(255,40,45,0.1);
    }

    .arch-node:last-child {
      border-color: var(--vivid-blue);
      box-shadow: 0 2px 8px rgba(59,130,246,0.1);
    }

    .arch-node.glow {
      border-color: var(--electric-violet);
      box-shadow: var(--glow-violet);
      transform: scale(1.05);
    }

    .arch-connector {
      flex: 1;
      height: 2px;
      background: linear-gradient(90deg, var(--fastly-red), var(--electric-violet), var(--vivid-blue));
      position: relative;
      min-width: 40px;
      opacity: 0.5;
    }

    .arch-connector-label {
      position: absolute;
      top: -16px;
      left: 50%;
      transform: translateX(-50%);
      font-size: 9px;
      color: #999;
      white-space: nowrap;
      font-weight: 500;
    }

    .arch-dot {
      position: absolute;
      width: 10px;
      height: 10px;
      background: var(--gradient-fire);
      border-radius: 50%;
      top: -4px;
      left: 0;
      opacity: 0;
      z-index: 2;
      box-shadow: 0 0 12px rgba(255,40,45,0.6);
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
      color: #888;
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
        <span class="arch-connector-label">EventBus</span>
        <div class="arch-dot" id="arch-dot-2"></div>
      </div>
      <div class="arch-node" id="eventbus-node">EventBus</div>
      <div class="arch-connector">
        <span class="arch-connector-label">SSE stream</span>
        <div class="arch-dot" id="arch-dot-3"></div>
      </div>
      <div class="arch-node" id="arch-client">Client</div>
    </div>
    <div class="arch-caption">
      In production, Fastly Fanout replaces the EventBus — holding connections at 90+ global edge PoPs
    </div>
  </div>

  <script>
    const PUBLISH_TOKEN = "demo-publish-token-fastly-fanout";
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
        dot.classList.remove("disconnected");
        status.textContent = "Connected";
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
      document.getElementById("connections-count").textContent = broadcastStats.subscribers;
      document.getElementById("payload-size").textContent = broadcastStats.payloadBytes + " bytes";
      document.getElementById("broadcast-latency").textContent = broadcastStats.broadcastMs + " ms";
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

    // --- Publish Actions ---
    async function publish(eventData) {
      animateArchDiagram();

      const res = await fetch("/publish", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Publish-Token": PUBLISH_TOKEN,
        },
        body: JSON.stringify(eventData),
      });

      const result = await res.json();
      if (result.broadcast) {
        updateDashboard(result.broadcast);
      }
      if (result.event) {
        handleEvent(result.event);
      }
    }

    document.getElementById("breaking-btn").addEventListener("click", () => {
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

    document.getElementById("score-btn").addEventListener("click", () => {
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

    // --- Init ---
    connectSSE();
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
