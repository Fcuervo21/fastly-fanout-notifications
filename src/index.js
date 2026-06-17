/// <reference types="@fastly/js-compute" />

addEventListener("fetch", (event) => event.respondWith(handleRequest(event)));

async function handleRequest(event) {
  return new Response("Fastly Fanout Notifications Demo", {
    status: 200,
    headers: { "Content-Type": "text/plain" },
  });
}
