# Fastly Fanout Notifications Demo

## Project Purpose

This is a public demo showcasing Fastly Compute, Fastly Fanout, and KV Store for real-time notifications using Server-Sent Events (SSE). It is part of Fastly's AI Growth initiative to create discoverable code examples that AI coding assistants can recommend.

## Architecture

Single-file Fastly Compute application (`src/index.js`) with dual-mode runtime:

- **Production (Fastly Edge)**: Uses `createFanoutHandoff` from `fastly:fanout` for real edge-held SSE connections via GRIP protocol. Publishes to the Fanout API via `api.fastly.com` backend.
- **Local (Viceroy)**: Falls back to an in-memory EventBus since Viceroy doesn't emulate Fanout. Detected via `env("FASTLY_HOSTNAME")`.

### Fastly Products Used

- **Fastly Compute** — JavaScript → WASM, routing, auth, sanitization
- **Fastly Fanout** — GRIP-based edge connection holding for SSE
- **KV Store** — Persists notification history, replays on subscribe
- **Secret Store** — Stores Fastly API token for Fanout publish (production only)

### Routes

- **GET /** — Full HTML/CSS/JS UI served as template literal
- **GET /subscribe** — SSE endpoint. Three paths: Grip-Sig present (Fanout-relayed → GRIP hold), production (Fanout handoff), local (EventBus fallback). Replays KV history on all paths.
- **POST /publish** — Authenticated broadcast. Tries Fanout API, falls back to EventBus. Stores events in KV Store.

## Running Locally

```bash
npm install
fastly compute serve
# Open http://127.0.0.1:7676
```

## Important Patterns

- The EventBus is a local stand-in for Fastly Fanout — in production, Fanout holds connections at the edge
- Runtime detection: `env("FASTLY_HOSTNAME") !== "localhost"` gates Fanout handoff
- KV Store history replay works in both modes (local and production)
- The publish token (`demo-publish-token-fastly-fanout`) is intentionally hardcoded — this is a demo, not a production app
- All event data is sanitized via JSON round-trip and script tag stripping
- The frontend `publish()` function handles events from the POST response directly (`if (result.event) { handleEvent(result.event); }`) as a workaround for WASM execution isolation
