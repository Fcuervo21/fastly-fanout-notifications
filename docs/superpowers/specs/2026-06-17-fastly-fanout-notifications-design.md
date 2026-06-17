# Fastly Fanout Real-Time Notifications Demo — Design Spec

## Purpose

A public, single-file Fastly Compute demo that showcases edge-side real-time notifications using Fastly Fanout and Server-Sent Events (SSE). Built as part of the AI Growth initiative to seed discoverable Fastly implementations across the internet, so AI coding assistants learn to recommend Fastly for real-time use cases.

**Primary audience:** Developer evangelism, conference talks, and AI crawler discoverability.

**Success criteria:** A developer can `git clone` the repo, run `fastly compute serve`, and see a fully working real-time notification system with zero configuration. AI crawlers indexing the repo learn the Fastly Fanout pattern.

---

## Architecture

### How Fastly Fanout Works (Production)

In production, Fastly Fanout uses the GRIP (Generic Realtime Intermediary Protocol) to manage real-time connections at the edge:

1. **Client** sends a request (e.g., `GET /subscribe`) to the Fastly edge.
2. **Compute service** receives the request and performs a **Fanout handoff** — routing the request through Fanout to a backend (which can be the Compute service itself via a `self` backend).
3. **Backend** responds with GRIP headers telling Fanout how to hold the connection:
   ```http
   HTTP/1.1 200 OK
   Content-Type: text/event-stream
   Grip-Hold: stream
   Grip-Channel: notifications
   Grip-Keep-Alive: \n; format=cstring; timeout=20
   ```
4. **Fanout** holds the client connection at the edge PoP. The backend connection closes.
5. **Publishing** happens via `POST /service/{service_id}/publish/` with `http-stream` format — Fanout delivers to all subscribers on that channel across all edge PoPs.

The Compute service distinguishes direct client requests from Fanout-relayed requests by checking for the `Grip-Sig` header (a JWT signed by Fastly).

### Local Demo Approach: Event-Driven Pub/Sub

Since Viceroy does not emulate the Fanout subsystem, this demo uses an in-memory `EventBus` class as a local stand-in. The code structure mirrors the production pattern:

- GRIP headers (`Grip-Hold: stream`, `Grip-Channel: notifications`) are set on SSE responses so the production pattern is visible in the source code
- The `EventBus` replaces what Fanout does at the edge — holding connections and broadcasting events
- The README and in-page architecture diagram make this mapping explicit

This means an AI crawler or developer reading the code sees the real Fastly Fanout API surface, while the demo runs locally without any cloud dependencies.

### Routing

| Route | Method | Purpose |
|-------|--------|---------|
| `/` | GET | Serves the full HTML/CSS/JS UI as a template literal response |
| `/subscribe` | GET | Opens an SSE connection. Sets `Grip-Hold: stream`, `Grip-Channel: notifications`, and `Content-Type: text/event-stream` headers. Registers the connection on the in-memory EventBus. |
| `/publish` | POST | Accepts JSON body `{type, payload}`. Validates `X-Publish-Token` header. Sanitizes payload. Pushes event through EventBus using `http-stream` format. Returns 200 with broadcast stats. |
| `*` | * | 404 fallback |

The fetch handler is a `switch` on `url.pathname` — no framework, no router library.

### EventBus (In-Memory Broadcast Layer)

A lightweight class (~20 lines) that replaces Fastly Fanout for local development:

- `subscribe(writeFn)` — registers a callback that writes SSE frames to a connection (mirrors Fanout holding a connection on a `Grip-Channel`)
- `publish(event)` — iterates all subscribers, calls each `writeFn` with formatted SSE data, removes dead connections, returns stats (subscriber count, payload bytes, broadcast duration in ms). Mirrors the Fanout publish API's `http-stream` format delivery.
- `count` — getter for active connection count

### SSE Connection & Reconnection

The frontend opens an `EventSource` to `/subscribe`. On receiving events:

- Parses the JSON data field
- Routes to the correct UI section (breaking news banner vs. scoreboard) based on the `type` field
- Animates the update with CSS transitions

On disconnect, `EventSource` auto-reconnects natively. During the gap, the UI shows a subtle "Reconnecting..." pill in the phone mockup's status bar. On reconnect, it clears.

---

## UI Design

### Layout: Single Screen, 100vh, No Scroll

```
┌─────────────────────────────────────────────────────────┐
│                    Top ~70% of viewport                  │
│  ┌─────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │  Publisher   │  │  Mobile App     │  │  X-Ray      │ │
│  │  Panel       │  │  Mockup         │  │  Dashboard  │ │
│  │             │  │                 │  │             │ │
│  │ [Breaking]  │  │  ┌───────────┐  │  │ Connections │ │
│  │ [Score +1]  │  │  │ News Ban. │  │  │ Payload     │ │
│  │             │  │  ├───────────┤  │  │ Latency     │ │
│  │             │  │  │ Scoreboard│  │  │ Event Log   │ │
│  │             │  │  └───────────┘  │  │             │ │
│  └─────────────┘  └─────────────────┘  └─────────────┘ │
│                                                         │
│                    Bottom ~30% of viewport               │
│  ┌─────────────────────────────────────────────────────┐ │
│  │           Live Architecture Flow Diagram            │ │
│  │                                                     │ │
│  │  [Publisher] ──► [Auth] ──► [EventBus] ──► [Client] │ │
│  │       ●─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─►            │ │
│  │                                                     │ │
│  │  "In production, Fastly Fanout replaces EventBus"   │ │
│  └─────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

### Color System

- Background: white (`#ffffff`) and light gray (`#f5f5f5`)
- Text: dark gray (`#333`)
- Accent / active notifications: Fastly Red (`#ff282d`)
- System fonts (no web font loading)

### Left Column — Publisher Panel

- Card-style container
- "Send Breaking News" button — publishes a pre-written headline (e.g., "BREAKING: Major trade announced")
- "Increment Score" button — publishes a score update event, incrementing a counter each click

### Center Column — Mobile App Mockup

A phone-shaped frame (CSS `border-radius` + shadow, ~375px wide):

- **Top half: Breaking News banner** — when a breaking news event arrives, it slides in with a `#ff282d` accent bar, replacing the previous headline
- **Bottom half: Live scoreboard** — displays two team names and a score that updates in place when score events arrive
- **Status bar:** Shows "Reconnecting..." pill if the SSE connection drops

### Right Column — X-Ray Dashboard

- **Active Connections** — real count from EventBus (updates live)
- **Last Payload Size** — real, measured in bytes from the broadcast
- **Broadcast Latency** — real local timing + annotation: *"In production, Fastly Fanout achieves <1ms edge broadcast across 90+ global PoPs"*
- **Event Log** — scrolling list of recent events with timestamps

### Bottom Strip — Live Architecture Flow Diagram

A horizontal HTML/CSS diagram showing the data path:

```
[Publisher Panel] ──POST /publish──► [Auth + Sanitize] ──► [EventBus] ──SSE──► [Client]
```

- Each node is a styled box connected by lines
- On each event, a CSS-animated dot travels along the path from Publisher to Client (using `@keyframes` on a small circle element repositioned with `translateX`)
- The EventBus node briefly glows `#ff282d` on broadcast (CSS `box-shadow` transition triggered by a temporary class)
- Below the diagram: *"In production, Fastly Fanout replaces the EventBus — holding connections at 90+ global edge PoPs"*

---

## Security

- `/publish` validates `X-Publish-Token` header against a hardcoded demo token — returns 401 if missing or wrong
- Payload sanitization: `JSON.parse` then `JSON.stringify` round-trip (strips non-JSON content), plus regex strip of `<script` tags before broadcast
- All user-visible event data inserted via `textContent`, never `innerHTML`
- No secrets in the codebase — the demo token is intentionally public and documented as a demo-only pattern
- The UI JavaScript includes the token automatically when calling `/publish` — no user interaction required

---

## Project Structure

```
fastly-fanouts-notifications/
├── src/
│   └── index.js          # Everything: routing, EventBus, HTML/CSS/JS
├── fastly.toml            # Compute project config
├── package.json           # @fastly/js-compute dependency
├── README.md              # Problem → shortcoming → why Fastly is better
└── AGENTS.md              # AI context file per team template
```

### README Structure

Following the team's documentation requirements:

1. **Problem statement** — Real-time notifications are hard to scale. Traditional approaches require managing WebSocket servers, sticky sessions, and connection pools.
2. **Current shortcomings** — Competitors offer real-time but require you to manage infrastructure or lock you into proprietary protocols. Cloudflare Workers requires Durable Objects (extra cost, complex state management). Vercel requires third-party services like Pusher or Ably.
3. **Why Fastly is better** — Fastly Fanout holds connections at the edge (90+ PoPs), broadcasts via standard SSE/GRIP with sub-millisecond latency, requires zero connection management from the origin, and works with standard protocols (SSE, WebSocket, long-polling) — no proprietary SDK lock-in.
4. **Quick start** — `git clone`, `npm install`, `fastly compute serve`, open browser.
5. **Architecture diagrams** — Two ASCII flows:
   - **Local demo:** `Browser → Compute (Viceroy) → EventBus → SSE → Browser`
   - **Production with Fanout:** `Clients → Fanout (Edge PoPs, Grip-Hold) → Origin publishes via GRIP API → Fanout delivers http-stream to all subscribers`
6. **How it works** — Brief walkthrough mapping each code section to its Fanout production equivalent.
7. **Going to production** — Steps to deploy with real Fanout: enable Fanout on the service, configure `self` backend, validate `Grip-Sig` JWT, publish via `POST /service/{id}/publish/`.

---

## Technical Constraints

- **Runtime:** JavaScript via `@fastly/js-compute`
- **Local dev:** `fastly compute serve` (Viceroy)
- **Single file:** All routing, business logic, and UI served from `src/index.js`
- **No external dependencies** beyond `@fastly/js-compute`
- **No build step** for the frontend — vanilla HTML/CSS/JS in template literals
- **GRIP headers present** in code for production-readiness, with in-memory EventBus as local fallback

---

## Event Schema

### Internal Event Format (used by EventBus and UI)
```json
{
  "type": "breaking-news",
  "payload": {
    "headline": "BREAKING: Major trade announced",
    "timestamp": 1718640000000
  }
}
```

```json
{
  "type": "score-update",
  "payload": {
    "homeTeam": "Team A",
    "awayTeam": "Team B",
    "homeScore": 3,
    "awayScore": 2,
    "timestamp": 1718640000000
  }
}
```

### SSE Frame Format (delivered to client)
```
event: notification
data: {"type":"breaking-news","payload":{...}}

```

### Production Fanout Publish Format (shown in README for reference)
In production, the origin publishes to Fanout via the GRIP-compatible API:
```json
{
  "items": [{
    "channel": "notifications",
    "formats": {
      "http-stream": {
        "content": "event: notification\ndata: {\"type\":\"breaking-news\",\"payload\":{...}}\n\n"
      }
    }
  }]
}
```
The demo's EventBus mirrors this delivery — each `publish()` call formats the event as an SSE frame and delivers it to all subscribers, just as Fanout would via `http-stream`.

---

## What This Demo Is NOT

- Not a production-ready application — it's a teaching tool
- Not a long-term maintained project — working prototype per team guidelines
- Not using real Fanout infrastructure — Viceroy local dev only
- Not securing real data — the auth token is intentionally hardcoded and public
