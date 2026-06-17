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
