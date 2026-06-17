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

addEventListener("fetch", (event) => event.respondWith(handleRequest(event)));

async function handleRequest(event) {
  const url = new URL(event.request.url);

  switch (url.pathname) {
    case "/subscribe":
      return handleSubscribe(event);
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
