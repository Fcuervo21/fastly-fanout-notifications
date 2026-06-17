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
      return new Response("Not Found", { status: 404 });
  }
}

function handleSubscribe(event) {
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const id = ++subscriberIdCounter;

  eventBus.subscribe(id, writer);

  const initMessage = `event: connected\ndata: ${JSON.stringify({ subscriberId: id })}\n\n`;
  writer.write(new TextEncoder().encode(initMessage));

  return new Response(readable, {
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
    return new Response("Method Not Allowed", { status: 405 });
  }

  const token = event.request.headers.get("X-Publish-Token");
  if (token !== PUBLISH_TOKEN) {
    return new Response(JSON.stringify({ error: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }

  let body;
  try {
    body = await event.request.json();
  } catch (e) {
    return new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  let sanitized;
  try {
    sanitized = sanitizeEvent(body);
  } catch (e) {
    return new Response(JSON.stringify({ error: e.message }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }

  const stats = eventBus.publish(sanitized);

  return new Response(
    JSON.stringify({
      success: true,
      event: sanitized,
      broadcast: stats,
    }),
    {
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
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

    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f5f5f5;
      color: #333;
      height: 100vh;
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }

    .top-bar {
      background: #fff;
      border-bottom: 1px solid #e0e0e0;
      padding: 12px 24px;
      display: flex;
      align-items: center;
      gap: 12px;
      flex-shrink: 0;
    }

    .top-bar h1 {
      font-size: 16px;
      font-weight: 600;
    }

    .top-bar .badge {
      background: #ff282d;
      color: #fff;
      font-size: 11px;
      font-weight: 600;
      padding: 2px 8px;
      border-radius: 10px;
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
      background: #fff;
      border-radius: 12px;
      border: 1px solid #e0e0e0;
      padding: 20px;
      display: flex;
      flex-direction: column;
      overflow: hidden;
    }

    .panel-title {
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: #999;
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
      border-radius: 8px;
      font-size: 14px;
      font-weight: 600;
      cursor: pointer;
      transition: transform 0.1s, box-shadow 0.2s;
    }

    .publish-btn:active {
      transform: scale(0.97);
    }

    .btn-breaking {
      background: #ff282d;
      color: #fff;
      box-shadow: 0 2px 8px rgba(255, 40, 45, 0.3);
    }

    .btn-breaking:hover {
      box-shadow: 0 4px 16px rgba(255, 40, 45, 0.4);
    }

    .btn-score {
      background: #333;
      color: #fff;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
    }

    .btn-score:hover {
      box-shadow: 0 4px 16px rgba(0, 0, 0, 0.25);
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
      border: 3px solid #222;
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.12);
      overflow: hidden;
      display: flex;
      flex-direction: column;
    }

    .phone-status-bar {
      background: #222;
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
      background: #4caf50;
      display: inline-block;
      margin-right: 6px;
    }

    .status-dot.disconnected {
      background: #ff282d;
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
      color: #999;
      margin-bottom: 8px;
    }

    .news-banner {
      background: #fff;
      border-left: 3px solid transparent;
      padding: 10px 12px;
      border-radius: 6px;
      font-size: 13px;
      font-weight: 600;
      line-height: 1.4;
      transition: all 0.3s ease;
      opacity: 0;
      transform: translateY(-10px);
    }

    .news-banner.active {
      border-left-color: #ff282d;
      background: #fff5f5;
      opacity: 1;
      transform: translateY(0);
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
    }

    .score-label {
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: #999;
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
      font-weight: 700;
      color: #333;
      transition: transform 0.2s ease;
    }

    .team-score.bumped {
      transform: scale(1.3);
      color: #ff282d;
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
      gap: 12px;
      margin-bottom: 16px;
    }

    .stat-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 8px 0;
      border-bottom: 1px solid #f0f0f0;
    }

    .stat-label {
      font-size: 12px;
      color: #999;
    }

    .stat-value {
      font-size: 14px;
      font-weight: 600;
      font-variant-numeric: tabular-nums;
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
      letter-spacing: 1px;
      color: #999;
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
      padding: 4px 0;
      border-bottom: 1px solid #f8f8f8;
      display: flex;
      gap: 8px;
    }

    .log-time {
      color: #999;
      flex-shrink: 0;
    }

    .log-type {
      color: #ff282d;
      font-weight: 600;
      flex-shrink: 0;
    }

    .log-detail {
      color: #666;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    /* Architecture Diagram */
    .arch-strip {
      flex: 3;
      background: #fff;
      border-top: 1px solid #e0e0e0;
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
      letter-spacing: 1px;
      color: #999;
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
      background: #f5f5f5;
      border: 1px solid #e0e0e0;
      border-radius: 8px;
      padding: 10px 16px;
      font-size: 12px;
      font-weight: 600;
      text-align: center;
      transition: box-shadow 0.3s, border-color 0.3s;
      z-index: 1;
      white-space: nowrap;
    }

    .arch-node.glow {
      border-color: #ff282d;
      box-shadow: 0 0 12px rgba(255, 40, 45, 0.4);
    }

    .arch-connector {
      flex: 1;
      height: 2px;
      background: #e0e0e0;
      position: relative;
      min-width: 40px;
    }

    .arch-connector-label {
      position: absolute;
      top: -16px;
      left: 50%;
      transform: translateX(-50%);
      font-size: 9px;
      color: #bbb;
      white-space: nowrap;
    }

    .arch-dot {
      position: absolute;
      width: 8px;
      height: 8px;
      background: #ff282d;
      border-radius: 50%;
      top: -3px;
      left: 0;
      opacity: 0;
      z-index: 2;
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
      color: #999;
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

  return new Response(html, {
    status: 200,
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}
