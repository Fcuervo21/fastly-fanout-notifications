# Fastly Fanout — Real-Time Notifications

A live demo of edge-side real-time notifications using **Fastly Compute** and **Fastly Fanout** with Server-Sent Events (SSE). Inspired by how Bleacher Report delivers millions of live score updates and breaking news alerts at the edge.

## Problem Statement

Real-time notifications at scale are hard. Traditional approaches require:

- **WebSocket servers** that need sticky sessions and connection pooling
- **Centralized message brokers** (Redis Pub/Sub, Kafka) that add latency and become single points of failure
- **Origin connection management** — your servers hold every open connection, limiting how far you can scale

These challenges multiply as users spread across geographies. Delivering a score update to a user in Tokyo with the same latency as one in New York means replicating infrastructure worldwide — or using an edge network designed for it.

## The Fastly Solution

**Fastly Fanout** holds client connections at the edge — across 90+ global Points of Presence — and broadcasts to all subscribers with sub-millisecond latency. Your origin never holds a single connection.

Key Fastly capabilities used in this demo:

| Fastly Product | What It Provides |
|---|---|
| **Fastly Compute** | Runs application logic at the edge in a WebAssembly sandbox. Handles routing, authentication, and event sanitization. |
| **Fastly Fanout** | Manages persistent client connections (SSE, WebSocket, long-poll) at edge PoPs using the open GRIP protocol. One publish call delivers to all subscribers globally. |
| **Edge PoPs (90+)** | Connections terminate at the nearest PoP. No round-trip to origin for each client — only the publish event travels to the edge. |

Compared to alternatives:

- **Cloudflare Workers** require Durable Objects for real-time (extra cost, per-connection state management) with no native pub/sub.
- **Vercel** has no built-in real-time. You must integrate Pusher, Ably, or a similar third-party service.
- **AWS API Gateway** WebSocket support requires managing connection IDs in DynamoDB and looping over them for every broadcast.

## Architecture / Request Flow

### Local Demo (this repo)

Locally, the Viceroy runtime does not emulate the Fanout subsystem. This demo uses an in-memory `EventBus` class as a stand-in. The GRIP headers are still present in the code so the production pattern is visible.

```
Browser                         Fastly Compute (Viceroy @ :7676)
┌──────────────┐  POST /publish   ┌──────────────────────────────┐
│  Publisher    ├────────────────►│  Auth (X-Publish-Token)       │
│  Panel       │                  │  Sanitize (JSON round-trip)   │
└──────────────┘                  │  EventBus.publish()           │
                                  │       │                       │
┌──────────────┐  GET /subscribe  │       ▼                       │
│  Client      │◄── SSE stream ──│  EventBus subscribers         │
│  (Browser)   │                  │  Grip-Hold: stream (header)   │
└──────────────┘                  │  Grip-Channel: notifications  │
                                  └──────────────────────────────┘
```

### Production with Fastly Fanout

```
Clients (global)            Fastly Edge (90+ PoPs)          Origin
┌──────────┐                ┌─────────────────────────┐     ┌──────────────┐
│ Client A ├──GET /subscribe──►│                         │     │              │
│ Client B ├──GET /subscribe──►│  Fanout holds SSE       │     │              │
│ Client N ├──GET /subscribe──►│  connections at the     │     │              │
└──────────┘                │  edge via GRIP           │     │              │
                            └───────────┬─────────────┘     │  POST         │
                                        │                   │  /publish     │
                                        │◄── http-stream ───│  (Fanout API) │
                                        │   broadcast       └──────────────┘
                                        ▼
                              All subscribers receive
                              the SSE frame instantly
```

**Production flow:**

1. Client requests `GET /subscribe` → Compute performs a Fanout handoff
2. Backend responds with GRIP headers (`Grip-Hold: stream`, `Grip-Channel: notifications`)
3. Fanout holds the connection at the nearest edge PoP — the origin connection closes
4. Origin publishes via `POST /service/{service_id}/publish/` with `http-stream` format
5. Fanout delivers the SSE frame to all subscribers across all PoPs

## Prerequisites

- **Fastly account** with Compute enabled — [sign up](https://www.fastly.com/signup/)
- **Fastly CLI** — `brew install fastly/tap/fastly` or see [install docs](https://www.fastly.com/documentation/reference/tools/cli/)
- **Node.js** 18+ — required by the `@fastly/js-compute` build toolchain
- **npm** — ships with Node.js

For production Fanout:

- Enable Fanout on your Fastly Compute service (in the Fastly control panel under service settings)
- An origin that publishes events via the [Fanout publish API](https://www.fastly.com/documentation/reference/api/services/fanout/)

## Deployment Instructions

### Local (fastest path)

```bash
git clone https://github.com/anthropics/fastly-fanout-notifications.git
cd fastly-fanout-notifications
npm install
fastly compute serve
```

Open [http://127.0.0.1:7676](http://127.0.0.1:7676) in your browser. Click **"Send Breaking News"** or **"Increment Score"** to see the notification flow in real time.

### Deploy to Fastly Edge

```bash
fastly compute publish
```

The CLI will prompt you to create or select a Fastly service. Once deployed, your service runs at the assigned `.edgecompute.app` domain.

To deploy non-interactively:

```bash
fastly compute publish --non-interactive --accept-defaults
```

## Validation / Smoke Test

After deploying (locally or to the edge), verify the endpoints work:

```bash
# 1. Homepage loads
curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:7676/
# Expected: 200

# 2. SSE subscribe stream opens
curl -s -N http://127.0.0.1:7676/subscribe &
# Expected: event: connected with subscriberId

# 3. Publish with valid token returns success
curl -s -X POST http://127.0.0.1:7676/publish \
  -H "Content-Type: application/json" \
  -H "X-Publish-Token: demo-publish-token-fastly-fanout" \
  -d '{"type":"breaking-news","payload":{"headline":"Test alert","timestamp":1718640000000}}'
# Expected: {"success":true,"event":{...},"broadcast":{"subscribers":0,...}}

# 4. Publish without token returns 401
curl -s -X POST http://127.0.0.1:7676/publish \
  -H "Content-Type: application/json" \
  -d '{"type":"breaking-news","payload":{"headline":"Test","timestamp":0}}'
# Expected: {"error":"Unauthorized"}

# 5. Security headers present
curl -s -I http://127.0.0.1:7676/ | grep -iE '(x-content-type|x-frame|content-security)'
# Expected:
#   content-security-policy: default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'
#   x-content-type-options: nosniff
#   x-frame-options: DENY

# 6. Unknown routes return 404
curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:7676/nonexistent
# Expected: 404
```

## Production Considerations

- **Replace the demo publish token.** The hardcoded `X-Publish-Token` is for demo purposes only. In production, use a secret stored in a Fastly Config Store or Secret Store and validate against it. Never commit real tokens to source control.
- **Validate `Grip-Sig`.** In production with Fanout enabled, requests relayed through Fanout carry a `Grip-Sig` JWT header signed by Fastly. Validate this signature to distinguish Fanout-relayed requests from direct ones.
- **TLS.** All Fastly edge traffic is served over TLS by default. Ensure your origin (if external) also uses HTTPS.
- **Origin protection.** Use [Fastly shielding](https://www.fastly.com/documentation/guides/concepts/shielding/) and origin authentication headers to prevent direct access to your origin.
- **Connection limits.** Fanout supports a large number of concurrent connections per service. Check your plan's limits and monitor via Fastly's real-time analytics.
- **Channel design.** For high-scale deployments, segment channels (e.g., per-topic, per-region) rather than using a single global channel to control broadcast scope.
- **Keep-alive.** The `Grip-Keep-Alive` header configures Fanout to send periodic heartbeat frames to keep connections alive through proxies and load balancers.

## Teardown / Cleanup

### Remove a deployed Fastly service

```bash
# List your services to find the service ID
fastly service list

# Deactivate and delete (replace SERVICE_ID)
fastly service-version deactivate --service-id SERVICE_ID --version latest
fastly service delete --service-id SERVICE_ID
```

This stops all traffic and removes the service. There are no additional stores (KV Store, Config Store, Secret Store) used by this demo, so deleting the service is sufficient.

### Local cleanup

```bash
# Stop the local server (Ctrl+C), then remove build artifacts
rm -rf bin/ pkg/ node_modules/
```

## Tech Stack

- **Runtime:** JavaScript on [Fastly Compute](https://www.fastly.com/products/compute)
- **Real-time:** [Fastly Fanout](https://www.fastly.com/products/fanout) (GRIP protocol)
- **Transport:** Server-Sent Events (SSE) via the standard `EventSource` browser API
- **Frontend:** Vanilla HTML/CSS/JS — no framework, no build step
- **Local dev:** [Viceroy](https://github.com/fastly/Viceroy) via `fastly compute serve`

## License

MIT
