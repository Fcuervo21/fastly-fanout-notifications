# Fastly Fanout — Real-Time Notifications

A live demo of edge-side real-time notifications using **Fastly Compute**, **Fastly Fanout**, and **KV Store** with Server-Sent Events (SSE). Inspired by how Bleacher Report delivers millions of live score updates and breaking news alerts at the edge.

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
| **Fastly Compute** | Runs application logic at the edge in a WebAssembly sandbox. Handles routing, authentication, and event sanitization. |
| **Fastly Fanout** | Manages persistent SSE connections at edge PoPs using the open GRIP protocol. One publish call delivers to all subscribers globally. |
| **KV Store** | Persists notification history at the edge. New subscribers receive recent events on connect. |
| **Secret Store** | Securely stores the Fastly API token used to publish to Fanout channels. |
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
                                        │                   │  /publish     │
                                        │◄── Fanout API ────│  → KV Store   │
                                        │   http-stream     │  → Fanout API │
                                        ▼                   └──────────────┘
                              All subscribers receive
                              the SSE frame instantly
```

**Production subscribe path:**

1. Client requests `GET /subscribe` (no `Grip-Sig` header)
2. Compute calls `createFanoutHandoff(req, "self")` — Fanout routes the request back with `Grip-Sig`
3. Compute detects `Grip-Sig`, responds with GRIP headers + replays KV Store history
4. Fanout holds the connection at the nearest edge PoP

**Production publish path:**

1. Origin receives `POST /publish` with auth token
2. Sanitizes event, stores in KV Store
3. Publishes to Fanout API (`POST /service/{id}/publish/`) with `http-stream` format
4. Fanout delivers the SSE frame to all subscribers across all PoPs

## Prerequisites

- **Fastly account** with Compute enabled — [sign up](https://www.fastly.com/signup/)
- **Fastly CLI** — `brew install fastly/tap/fastly` or see [install docs](https://www.fastly.com/documentation/reference/tools/cli/)
- **Node.js** 18+ — required by the `@fastly/js-compute` build toolchain
- **npm** — ships with Node.js

For production Fanout:

- Enable Fanout on your Fastly Compute service (in the Fastly control panel under service settings)
- Create a KV Store named `notification-history` (provisioned automatically by `fastly compute publish`)
- Create a Secret Store named `fanout-secrets` with a `fastly-api-token` entry containing a Fastly API token with publish permissions

## Deployment Instructions

### Local (fastest path)

```bash
git clone https://github.com/Fcuervo21/fastly-fanout-notifications.git
cd fastly-fanout-notifications
npm install
fastly compute serve
```

Open [http://127.0.0.1:7676](http://127.0.0.1:7676) in your browser. Click **"Send Breaking News"** or **"Increment Score"** to see the notification flow in real time.

### Deploy to Fastly Edge

```bash
fastly compute publish
```

The CLI will prompt you to create or select a Fastly service and automatically provision the `notification-history` KV Store. Once deployed, your service runs at the assigned `.edgecompute.app` domain.

To deploy non-interactively:

```bash
fastly compute publish --non-interactive --accept-defaults
```

### Enable Fanout (production real-time)

After deploying, enable Fanout for cross-client real-time delivery:

1. In the [Fastly control panel](https://manage.fastly.com/), navigate to your service settings
2. Enable Fanout
3. Create a Secret Store named `fanout-secrets`:
   ```bash
   fastly secret-store create --name fanout-secrets
   fastly secret-store-entry create --store-id <STORE_ID> --name fastly-api-token --secret <YOUR_API_TOKEN>
   fastly resource-link create --service-id <SERVICE_ID> --version latest --resource-id <STORE_ID>
   ```
4. Activate the new version

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

# 3. Publish with valid token returns success
curl -s -X POST http://127.0.0.1:7676/publish \
  -H "Content-Type: application/json" \
  -H "X-Publish-Token: demo-publish-token-fastly-fanout" \
  -d '{"type":"breaking-news","payload":{"headline":"Test alert","timestamp":1718640000000}}'
# Expected: {"success":true,"event":{...},"broadcast":{...,"mode":"local"}}

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

- **Replace the demo publish token.** The hardcoded `X-Publish-Token` is for demo purposes only. In production, validate against a secret stored in a Fastly Secret Store.
- **Validate `Grip-Sig`.** In production with Fanout enabled, requests relayed through Fanout carry a `Grip-Sig` JWT header signed by Fastly. Validate this signature to distinguish Fanout-relayed requests from direct ones.
- **TLS.** All Fastly edge traffic is served over TLS by default. Ensure your origin (if external) also uses HTTPS.
- **Origin protection.** Use [Fastly shielding](https://www.fastly.com/documentation/guides/concepts/shielding/) and origin authentication headers to prevent direct access to your origin.
- **KV Store consistency.** KV Store is eventually consistent. A just-published event might not appear in the history for a subscriber connecting milliseconds later. The frontend handles this by also processing events from the publish response directly.
- **Channel design.** For high-scale deployments, segment channels (e.g., per-topic, per-region) rather than using a single global channel.
- **Keep-alive.** The `Grip-Keep-Alive` header configures Fanout to send periodic heartbeat frames to keep connections alive through proxies and load balancers.

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
# List KV stores to find the store ID
fastly kv-store list

# Delete the notification history store
fastly kv-store delete --store-id STORE_ID
```

### Remove the Secret Store (if created)

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
- **Storage:** [Fastly KV Store](https://www.fastly.com/products/kv-store) for notification history
- **Secrets:** [Fastly Secret Store](https://docs.fastly.com/en/guides/working-with-secret-stores) for API tokens
- **Transport:** Server-Sent Events (SSE) via the standard `EventSource` browser API
- **Frontend:** Vanilla HTML/CSS/JS — no framework, no build step
- **Local dev:** [Viceroy](https://github.com/fastly/Viceroy) via `fastly compute serve`

## License

MIT
