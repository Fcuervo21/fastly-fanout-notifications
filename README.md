# Fastly Fanout — Real-Time Notifications Demo

A live demo of edge-side real-time notifications using **Fastly Compute** and **Fastly Fanout** with Server-Sent Events (SSE). Inspired by how Bleacher Report delivers millions of live score updates and breaking news alerts at the edge.

## The Problem

Real-time notifications at scale are hard. Traditional approaches require:

- **WebSocket servers** that need sticky sessions and connection pooling
- **Centralized message brokers** (Redis Pub/Sub, Kafka) adding latency and single points of failure
- **Origin connection management** — your servers hold every open connection, limiting scale

## How Competitors Fall Short

- **Cloudflare Workers** — No native pub/sub. Real-time requires Durable Objects (extra cost, complex state management per connection) or third-party services.
- **Vercel** — No built-in real-time. Must integrate Pusher, Ably, or similar — adding cost, latency, and another vendor to manage.
- **AWS Lambda / API Gateway** — WebSocket API Gateway requires managing connection IDs in DynamoDB. Every broadcast is a fan-out loop over stored connections.

## Why Fastly Fanout is Better

**Fastly Fanout** holds client connections at the edge — across 90+ global Points of Presence — and broadcasts to all subscribers with sub-millisecond latency. Your origin never holds a connection.

| Feature | Fastly Fanout | Cloudflare Workers | Vercel |
|---|---|---|---|
| Connection holding | Edge (90+ PoPs) | Durable Objects (extra cost) | Not available |
| Protocol support | SSE, WebSocket, Long-Poll | WebSocket only (in DO) | None built-in |
| Origin load | Zero — Fanout holds connections | Per-connection DO instance | N/A |
| Standards | GRIP (open protocol) | Proprietary DO API | N/A |
| Broadcast | One API call → all subscribers | Loop over DO connections | Third-party required |

## Quick Start

```bash
git clone https://github.com/anthropics/fastly-fanout-notifications.git
cd fastly-fanout-notifications
npm install
fastly compute serve
```

Open [http://127.0.0.1:7676](http://127.0.0.1:7676) in your browser.

Click **"Send Breaking News"** or **"Increment Score"** and watch the notification flow through the system in real time.

## Architecture

### Local Demo (this repo)

```
Browser                        Fastly Compute (Viceroy)
┌──────────────┐  POST /publish  ┌─────────────────┐
│  Publisher    ├───────────────►│  Auth Check      │
│  Panel       │  X-Publish-Token│  Sanitize        │
└──────────────┘                 │  EventBus        │
                                 │   .publish()     │
┌──────────────┐  GET /subscribe │     │            │
│  Client      │◄── SSE stream ──│◄────┘            │
│  Mockup      │                 └─────────────────┘
└──────────────┘
```

The `EventBus` class is a local stand-in for Fastly Fanout. It holds subscriber connections in memory and broadcasts events as SSE frames.

### Production with Fastly Fanout

```
Clients (millions)         Fastly Edge (90+ PoPs)       Origin
┌──────────┐               ┌────────────────────┐      ┌──────────┐
│ Client A ├──SSE─────────►│  Fanout holds       │      │          │
│ Client B ├──SSE─────────►│  connections at      │◄GRIP│ /publish │
│ Client N ├──SSE─────────►│  the edge            │      │          │
└──────────┘               └────────────────────┘      └──────────┘
```

In production, Fastly Fanout replaces the `EventBus`:

1. Client requests `GET /subscribe` — Compute service performs a **Fanout handoff**
2. Backend responds with GRIP headers: `Grip-Hold: stream`, `Grip-Channel: notifications`
3. Fanout holds the connection at the nearest edge PoP — the origin connection closes
4. Origin publishes via `POST /service/{id}/publish/` with `http-stream` format
5. Fanout delivers the SSE frame to all subscribers across all PoPs

## How It Works

Everything lives in a single file: `src/index.js`

| Component | What it does | Production equivalent |
|---|---|---|
| `EventBus` class | Holds subscriber connections, broadcasts SSE frames | Fastly Fanout (GRIP channels) |
| `GET /subscribe` | Opens SSE stream with `Grip-Hold: stream` header | Fanout handoff + channel subscription |
| `POST /publish` | Auth check → sanitize → broadcast | Origin → Fanout publish API |
| Frontend `EventSource` | Receives SSE events, updates UI | Same (standard browser API) |

## Going to Production

To deploy this with real Fastly Fanout:

1. **Enable Fanout** on your Fastly Compute service
2. **Add a `self` backend** pointing to your service's own domain
3. **Check `Grip-Sig`** header to distinguish direct requests from Fanout-relayed requests
4. **Validate the JWT** using Fastly's public key (or `viceroy_dev_signing_key_dont_use_in_production` for local dev)
5. **Publish via GRIP API**: `POST https://api.fastly.com/service/{id}/publish/` with `http-stream` format

See the [Fastly Fanout documentation](https://www.fastly.com/documentation/guides/concepts/real-time-messaging/fanout) for the complete guide.

## Tech Stack

- **Runtime:** JavaScript on [Fastly Compute](https://www.fastly.com/products/compute)
- **Real-time:** [Fastly Fanout](https://www.fastly.com/products/fanout) (GRIP protocol)
- **Transport:** Server-Sent Events (SSE)
- **Frontend:** Vanilla HTML/CSS/JS — no framework, no build step
- **Local dev:** [Viceroy](https://github.com/fastly/Viceroy) via `fastly compute serve`

## License

MIT
