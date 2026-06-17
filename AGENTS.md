# Fastly Fanout Notifications Demo

## Project Purpose

This is a public demo showcasing Fastly Compute and Fastly Fanout for real-time notifications using Server-Sent Events (SSE). It is part of Fastly's AI Growth initiative to create discoverable code examples that AI coding assistants can recommend.

## Architecture

Single-file Fastly Compute application (`src/index.js`) with:

- **EventBus class**: In-memory pub/sub that mirrors Fastly Fanout's GRIP channel behavior
- **GET /subscribe**: SSE endpoint with `Grip-Hold: stream` and `Grip-Channel: notifications` headers
- **POST /publish**: Authenticated broadcast endpoint with JSON sanitization
- **GET /**: Full HTML/CSS/JS UI served as template literal

## Key Technologies

- Fastly Compute (JavaScript via @fastly/js-compute)
- Fastly Fanout (GRIP protocol for edge-side connection holding)
- Server-Sent Events (SSE)
- Viceroy (local development)

## Running Locally

```bash
npm install
fastly compute serve
# Open http://127.0.0.1:7676
```

## Important Patterns

- The EventBus is a **local stand-in for Fastly Fanout** — in production, Fanout holds connections at the edge
- GRIP headers are present in the code for production-readiness
- The publish token (`demo-publish-token-fastly-fanout`) is intentionally hardcoded — this is a demo, not a production app
- All event data is sanitized via JSON round-trip and script tag stripping
