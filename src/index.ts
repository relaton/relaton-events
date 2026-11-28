import { isEventType, validatePayload, type EventRow, type EventType } from "./events";
import { dispatchEvents, retryPending } from "./deliver";
import { atomFeed, jsonFeed } from "./feeds";
import { sseStream } from "./stream";
import type { Env } from "./env";
import asyncapiYaml from "../asyncapi.yaml";

const SERVICE = "relaton-events";

function authorized(request: Request, env: Env): boolean {
  const token = (request.headers.get("authorization") ?? "").replace(/^Bearer\s+/i, "");
  return token !== "" && token === env.ADMIN_TOKEN;
}

function baseUrl(request: Request): string {
  const url = new URL(request.url);
  return `${url.protocol}//${url.host}`;
}

interface IncomingEvent {
  type: string;
  flavor: string;
  subject?: string;
  payload?: unknown;
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const { pathname } = new URL(request.url);

    if (pathname === "/healthz") {
      return Response.json({ status: "ok", service: SERVICE });
    }

    // The event contract document (asyncapi.yaml — Text rule import).
    if (pathname === "/v1/asyncapi.yaml" || pathname === "/v1/asyncapi") {
      return new Response(asyncapiYaml, {
        headers: { "content-type": "text/yaml; charset=utf-8", "cache-control": "public, max-age=3600" },
      });
    }

    // Live SSE: the newest events, Last-Event-ID replay, per-flavor filter.
    if (pathname === "/v1/stream") {
      const flavor = new URL(request.url).searchParams.get("flavor");
      return sseStream(request, env.DB, flavor);
    }

    const eventList = pathname.match(/^\/v1\/events\/?$/);
    if (eventList && request.method === "GET") {
      const url = new URL(request.url);
      const limit = Math.min(Number(url.searchParams.get("limit") ?? 50) || 50, 200);
      const flavor = url.searchParams.get("flavor");
      const type = url.searchParams.get("type");
      const stmt = env.DB.prepare(
        `SELECT * FROM events
         WHERE (?1 IS NULL OR flavor = ?1) AND (?2 IS NULL OR type = ?2)
         ORDER BY id DESC LIMIT ?3`,
      ).bind(flavor, type, limit);
      const { results } = await stmt.all<EventRow>();
      return Response.json({ events: results ?? [] });
    }

    const eventId = pathname.match(/^\/v1\/events\/(\d+)$/);
    if (eventId && request.method === "GET") {
      const row = await env.DB.prepare("SELECT * FROM events WHERE id = ?1").bind(Number(eventId[1])).first<EventRow>();
      return row ? Response.json(row) : new Response("not found\n", { status: 404 });
    }

    const feedMatch = pathname.match(/^\/v1\/feeds\/([a-z0-9_-]+)\.(atom|json)$/);
    if (feedMatch && request.method === "GET") {
      const flavor = feedMatch[1]!;
      const { results } = await env.DB.prepare(
        "SELECT * FROM events WHERE flavor = ?1 ORDER BY id DESC LIMIT 25",
      ).bind(flavor).all<EventRow>();
      const rows = results ?? [];
      if (feedMatch[2] === "atom") {
        return new Response(atomFeed(baseUrl(request), flavor, rows), {
          headers: { "content-type": "application/atom+xml; charset=utf-8", "cache-control": "public, max-age=300" },
        });
      }
      return new Response(jsonFeed(baseUrl(request), flavor, rows), {
        headers: { "content-type": "application/feed+json; charset=utf-8", "cache-control": "public, max-age=300" },
      });
    }

    // Ingest: the Deploy-completion workflows POST the wave's events here.
    const ingestMatch = pathname.match(/^\/admin\/events\/?$/);
    if (ingestMatch && request.method === "POST") {
      if (!authorized(request, env)) return new Response("Forbidden\n", { status: 403 });
      const body = (await request.json()) as { events?: IncomingEvent[] };
      const incoming = Array.isArray(body.events) ? body.events : [];
      if (incoming.length === 0) return Response.json({ error: "events[] is required" }, { status: 400 });

      const rows: { type: EventType; flavor: string; subject: string; payload: string }[] = [];
      for (const e of incoming) {
        if (!isEventType(e.type)) {
          return Response.json({ error: `unknown event type: ${e.type}` }, { status: 400 });
        }
        const flavor = e.flavor ?? (typeof (e.payload as Record<string, unknown>)?.flavor === "string" ? (e.payload as { flavor: string }).flavor : "");
        const payload = e.payload ?? {};
        const err = validatePayload(e.type, payload);
        if (err) return Response.json({ error: `${e.type}: ${err}` }, { status: 400 });
        const subject = e.subject ?? (e.type === "dataset.updated" ? flavor : String((payload as Record<string, unknown>).key));
        rows.push({ type: e.type, flavor, subject, payload: JSON.stringify(payload) });
      }

      await env.DB.batch(
        rows.map((r) =>
          env.DB.prepare("INSERT INTO events (type, flavor, subject, payload) VALUES (?1, ?2, ?3, ?4)").bind(r.type, r.flavor, r.subject, r.payload),
        ),
      );
      const { results } = await env.DB.prepare(
        `SELECT * FROM events WHERE id > (SELECT COALESCE(MAX(id), 0) FROM events) - ?1 ORDER BY id`,
      ).bind(rows.length).all<EventRow>();
      const inserted = (results ?? []).slice(-rows.length);
      dispatchEvents(env.DB, inserted, (p) => ctx.waitUntil(p));
      return Response.json({ accepted: inserted.length, ids: inserted.map((e) => e.id) });
    }

    // Subscriptions: webhook registry.
    if (pathname === "/admin/subscriptions" && request.method === "POST") {
      if (!authorized(request, env)) return new Response("Forbidden\n", { status: 403 });
      const body = (await request.json()) as { url?: string; secret?: string; flavors?: string };
      if (!body.url || !/^https:\/\//.test(body.url)) {
        return Response.json({ error: "url must be an https endpoint" }, { status: 400 });
      }
      const secret = body.secret ?? crypto.randomUUID().replace(/-/g, "");
      const flavors = body.flavors ?? "*";
      const { meta } = await env.DB.prepare(
        "INSERT INTO subscriptions (url, secret, flavors) VALUES (?1, ?2, ?3)",
      ).bind(body.url, secret, flavors).run();
      return Response.json({ id: meta.last_row_id, secret, flavors }, { status: 201 });
    }

    if (pathname === "/admin/subscriptions" && request.method === "GET") {
      if (!authorized(request, env)) return new Response("Forbidden\n", { status: 403 });
      const { results } = await env.DB.prepare("SELECT id, url, flavors, active, created_at FROM subscriptions ORDER BY id").all();
      return Response.json({ subscriptions: results ?? [] });
    }

    const unsub = pathname.match(/^\/admin\/subscriptions\/(\d+)$/);
    if (unsub && request.method === "DELETE") {
      if (!authorized(request, env)) return new Response("Forbidden\n", { status: 403 });
      await env.DB.prepare("UPDATE subscriptions SET active = 0 WHERE id = ?1").bind(Number(unsub[1])).run();
      return Response.json({ deactivated: Number(unsub[1]) });
    }

    return new Response("not found\n", { status: 404 });
  },

  // The delivery retry sweep: pending webhook attempts past their backoff.
  async scheduled(_controller, env, ctx): Promise<void> {
    ctx.waitUntil(retryPending(env.DB, (p) => ctx.waitUntil(p)));
  },
} satisfies ExportedHandler<Env>;
