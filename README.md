# Fastly Fanout — Real-Time Notifications

A live demo of edge-side real-time notifications using **Fastly Compute**, **Fastly Fanout**, and **KV Store** with Server-Sent Events (SSE). Inspired by how Bleacher Report delivers millions of live score updates and breaking news alerts at the edge.

**Live demo:** [https://personally-exact-frog.edgecompute.app](https://personally-exact-frog.edgecompute.app)

## Problem Statement

Real-time notifications at scale are hard. Traditional approaches require:

- **WebSocket servers** that need sticky sessions and connection pooling
- **Centralized message brokers** (Redis Pub/Sub, Kafka) that add latency and become single points of failure
- **Origin connection management** — your servers hold every open connection, limiting how far you can scale

These challenges multiply as users spread across geographies. Delivering a score update to a user in Tokyo with the same latency as one in New York means replicating infrastructure worldwide — or using an edge network designed for it.

## The Fastly Solution

**Fastly Fanout** holds client connections at the edge — across 90+ global Points of Presence — and broadcasts to all subscribers with sub-millisecond latency. Your origin never holds a single connection.

| Fastly Product | What It Provides |
|---|---|
| **Fastly Compute** | Runs application logic at the edge in a WebAssembly sandbox. Handles routing, authentication, event sanitization, and rate limiting. |
| **Fastly Fanout** | Manages persistent SSE connections at edge PoPs using the open GRIP protocol. One publish call delivers to all subscribers globally. |
| **KV Store** | Persists notification history for replay on connect. Also enforces per-IP budgets and global rate limits server-side. |
| **Secret Store** | Securely stores the Fastly API token used to publish to Fanout channels. No secrets are exposed in client-side code. |
| **Edge PoPs (90+)** | Connections terminate at the nearest PoP. No round-trip to origin for each client. |

Compared to alternatives:

- **Cloudflare Workers** require Durable Objects for real-time (extra cost, per-connection state management) with no native pub/sub.
- **Vercel** has no built-in real-time. You must integrate Pusher, Ably, or a similar third-party service.
- **AWS API Gateway** WebSocket support requires managing connection IDs in DynamoDB and looping over them for every broadcast.

## Architecture / Request Flow

### Dual-Mode Runtime

This demo runs in two modes with automatic detection:

- **Production (Fastly Edge)**: Uses `createFanoutHandoff` for real edge-held SSE connections via the GRIP protocol. Publishes to Fanout via the `api.fastly.com` backend.
- **Local (Viceroy)**: Falls back to an in-memory EventBus since Viceroy doesn't emulate Fanout. Detected via `env("FASTLY_HOSTNAME")`.

### Local Demo Flow

```
Browser                         Fastly Compute (Viceroy @ :7676)
┌──────────────┐  POST /publish   ┌──────────────────────────────┐
│  Publisher    ├────────────────►│  Auth (X-Publish-Token)       │
│  Panel       │                  │  Sanitize → KV Store (write)  │
└──────────────┘                  │  EventBus.publish()           │
                                  │       │                       │
┌──────────────┐  GET /subscribe  │       ▼                       │
│  Client      │◄── SSE stream ──│  EventBus subscribers         │
│  (Browser)   │                  │  + KV Store history replay    │
└──────────────┘                  └──────────────────────────────┘
```

### Production Flow (Fastly Edge)

```
Clients (global)            Fastly Edge (90+ PoPs)          Origin
┌──────────┐                ┌─────────────────────────┐     ┌──────────────┐
│ Client A ├──GET /subscribe──►│  Fanout handoff           │     │              │
│ Client B ├──GET /subscribe──►│  → Grip-Sig relayed       │     │              │
│ Client N ├──GET /subscribe──►│  → GRIP hold at edge      │     │              │
└──────────┘                │  + KV history replay      │     │              │
                            └───────────┬─────────────┘     │  POST         │
                                        │                   │  /demo-publish│
                                        │◄── Fanout API ────│  → KV budget  │
                                        │   http-stream     │  → KV Store   │
                                        ▼                   │  → Fanout API │
                              All subscribers receive       └──────────────┘
                              the SSE frame instantly
```

**Production subscribe path:**

1. Client requests `GET /subscribe` (no `Grip-Sig` header)
2. Compute calls `createFanoutHandoff(req, "self")` — Fanout routes the request back with `Grip-Sig`
3. Compute detects `Grip-Sig`, responds with GRIP headers + replays KV Store history
4. Fanout holds the connection at the nearest edge PoP

**Production publish path:**

1. Browser sends `POST /demo-publish` (no token needed — budget enforced server-side)
2. Server checks per-IP budget (5/hour) and global rate limit (10/min) via KV Store
3. Sanitizes event, stores in KV Store for history replay
4. Publishes to Fanout API (`POST /service/{id}/publish/`) with `http-stream` format
5. Fanout delivers the SSE frame to all subscribers across all PoPs

### Demo Resource Protection

This demo is designed to be safe to leave running publicly:

| Layer | Mechanism | Limit |
|---|---|---|
| **Per-IP budget** | KV Store keyed by IP hash + hour | 5 interactive clicks per IP per hour |
| **Global rate limit** | KV Store keyed by minute | 10 publishes per minute across all visitors |
| **Auto-demo mode** | Client-side JS | After budget exhaustion, simulated events stream locally at zero server cost |
| **No exposed secrets** | Secret Store | Publish token and API keys are never in the HTML source |

The frontend calls `/demo-publish` (no auth token). The authenticated `/publish` endpoint exists for API use and validates tokens against the Secret Store.

### Endpoints

| Route | Method | Auth | Description |
|---|---|---|---|
| `/` | GET | None | Full HTML/CSS/JS UI |
| `/subscribe` | GET | None | SSE stream — Fanout in production, EventBus locally. Replays KV history on connect. |
| `/demo-publish` | POST | Per-IP budget (KV Store) | Browser-facing publish. Budget-limited, no token needed. |
| `/publish` | POST | `X-Publish-Token` header | API publish. Token validated against Secret Store (production) or hardcoded fallback (local). |

## Prerequisites

- **Fastly account** with Compute enabled — [sign up](https://www.fastly.com/signup/)
- **Fastly CLI** — `brew install fastly/tap/fastly` or see [install docs](https://www.fastly.com/documentation/reference/tools/cli/)
- **Node.js** 18+ — required by the `@fastly/js-compute` build toolchain
- **npm** — ships with Node.js

## Deployment Instructions

### Local (fastest path)

```bash
git clone https://github.com/Fcuervo21/fastly-fanout-notifications.git
cd fastly-fanout-notifications
npm install
fastly compute serve
```

Open [http://127.0.0.1:7676](http://127.0.0.1:7676) in your browser. Click **"Send Breaking News"** or **"Increment Score"** to see the notification flow in real time. Locally, the demo uses EventBus (in-memory) and the hardcoded publish token.

### Deploy to Fastly Edge

```bash
fastly compute publish
```

The CLI will prompt you to create or select a Fastly service. Once deployed, your service runs at the assigned `.edgecompute.app` domain.

**After the first deploy, you must manually add two backends** (the `[setup.backends]` in `fastly.toml` only runs on initial service creation):

```bash
# Clone the active version to edit it
fastly service-version clone --service-id <SERVICE_ID> --version active

# Add the self backend (Fanout routes requests back to your service)
fastly backend create --service-id <SERVICE_ID> --version <NEW_VERSION> \
  --name self \
  --address <YOUR_DOMAIN>.edgecompute.app --port 443 \
  --use-ssl --ssl-sni-hostname <YOUR_DOMAIN>.edgecompute.app \
  --override-host <YOUR_DOMAIN>.edgecompute.app

# Add the Fanout publish backend (for broadcasting events)
fastly backend create --service-id <SERVICE_ID> --version <NEW_VERSION> \
  --name fanout_publish \
  --address api.fastly.com --port 443 \
  --use-ssl --ssl-sni-hostname api.fastly.com \
  --override-host api.fastly.com

# Activate the version with backends
fastly service-version activate --service-id <SERVICE_ID> --version <NEW_VERSION>
```

### Enable Fanout (production real-time)

1. In the [Fastly control panel](https://manage.fastly.com/), navigate to your service → **Settings** → **Product Enablement** → toggle **Fanout** on
2. Create a **KV Store** named `notification_history` and link it to your service
3. Create a **Secret Store** named `fastly_fanout` and link it to your service, with these entries:
   - `fastly-api-token` — a Fastly API token with publish permissions
   - `publish-token` (optional) — a custom token for the authenticated `/publish` endpoint
4. Activate the new service version

## Validation / Smoke Test

After deploying (locally or to the edge), verify the endpoints work:

```bash
# 1. Homepage loads
curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:7676/
# Expected: 200

# 2. SSE subscribe stream opens with history replay
curl -s -N --max-time 3 http://127.0.0.1:7676/subscribe
# Expected: event: connected with mode: "local" (or "fanout" on edge)
# Followed by any recently published events from KV Store

# 3. Demo publish (no token needed, budget-limited)
curl -s -X POST http://127.0.0.1:7676/demo-publish \
  -H "Content-Type: application/json" \
  -d '{"type":"breaking-news","payload":{"headline":"Test alert","timestamp":1718640000000}}'
# Expected: {"success":true,"event":{...},"broadcast":{...,"mode":"local"}}

# 4. Authenticated publish with token
curl -s -X POST http://127.0.0.1:7676/publish \
  -H "Content-Type: application/json" \
  -H "X-Publish-Token: demo-publish-token-fastly-fanout" \
  -d '{"type":"breaking-news","payload":{"headline":"Test","timestamp":1718640000000}}'
# Expected: {"success":true,...}

# 5. Publish without token returns 401
curl -s -X POST http://127.0.0.1:7676/publish \
  -H "Content-Type: application/json" \
  -d '{"type":"breaking-news","payload":{"headline":"Test","timestamp":0}}'
# Expected: {"error":"Unauthorized"}

# 6. Security headers present
curl -s -I http://127.0.0.1:7676/ | grep -iE '(x-content-type|x-frame|content-security)'
# Expected:
#   content-security-policy: default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'
#   x-content-type-options: nosniff
#   x-frame-options: DENY

# 7. Unknown routes return 404
curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:7676/nonexistent
# Expected: 404
```

## Production Considerations

- **Validate `Grip-Sig`.** In production with Fanout enabled, requests relayed through Fanout carry a `Grip-Sig` JWT header signed by Fastly. Validate this signature to distinguish Fanout-relayed requests from direct ones.
- **TLS.** All Fastly edge traffic is served over TLS by default. Ensure your origin (if external) also uses HTTPS.
- **Origin protection.** Use [Fastly shielding](https://www.fastly.com/documentation/guides/concepts/shielding/) and origin authentication headers to prevent direct access to your origin.
- **KV Store consistency.** KV Store is eventually consistent. A just-published event might not appear in the history for a subscriber connecting milliseconds later.
- **Channel design.** For high-scale deployments, segment channels (e.g., per-topic, per-region) rather than using a single global channel.
- **Keep-alive.** The `Grip-Keep-Alive` header configures Fanout to send periodic heartbeat frames to keep connections alive through proxies and load balancers.
- **Backend persistence.** `fastly compute publish` creates a fresh service version. If your backends were added manually (not via `[setup]`), clone the active version after publish and activate the clone to preserve them.

## Teardown / Cleanup

### Remove a deployed Fastly service

```bash
# List your services to find the service ID
fastly service list

# Deactivate and delete
fastly service-version deactivate --service-id SERVICE_ID --version latest
fastly service delete --service-id SERVICE_ID
```

### Remove the KV Store

```bash
fastly kv-store list
fastly kv-store delete --store-id STORE_ID
```

### Remove the Secret Store

```bash
fastly secret-store list
fastly secret-store delete --store-id STORE_ID
```

### Local cleanup

```bash
rm -rf bin/ pkg/ node_modules/
```

## Tech Stack

- **Runtime:** JavaScript on [Fastly Compute](https://www.fastly.com/products/compute)
- **Real-time:** [Fastly Fanout](https://www.fastly.com/products/fanout) (GRIP protocol)
- **Storage:** [Fastly KV Store](https://www.fastly.com/products/kv-store) — notification history, per-IP budgets, rate limiting
- **Secrets:** [Fastly Secret Store](https://docs.fastly.com/en/guides/working-with-secret-stores) — API tokens, publish auth
- **Transport:** Server-Sent Events (SSE) via the standard `EventSource` browser API
- **Frontend:** Vanilla HTML/CSS/JS — no framework, no build step
- **Local dev:** [Viceroy](https://github.com/fastly/Viceroy) via `fastly compute serve`

## License

MIT
