import { beforeAll, describe, expect, it } from "vitest";
import { SELF, env } from "cloudflare:test";

// relaton-events#1 contract suite. The pool has no outbound-fetch mock in
// this version, so delivery is tested through attemptDelivery's injected
// fetcher; the route-level flow is covered to the delivery-cursor rows.

const { DB } = (env as unknown as { DB: D1Database });

beforeAll(async () => {
  const schema = (await import("../migrations/0001_init.sql?raw")).default;
  for (const s of schema.split(";").map((s2: string) => s2.trim()).filter(Boolean)) {
    await DB.prepare(s).run();
  }
});

function auth(): Record<string, string> {
  return { authorization: "Bearer test-admin", "content-type": "application/json" };
}

async function insertEvent(type: string, flavor: string, subject: string, payload: unknown): Promise<number> {
  const { meta } = await DB.prepare("INSERT INTO events (type, flavor, subject, payload) VALUES (?1, ?2, ?3, ?4)")
    .bind(type, flavor, subject, JSON.stringify(payload))
    .run();
  return meta.last_row_id!;
}

describe("health + contract", () => {
  it("reports ok", async () => {
    const res = await SELF.fetch("https://events.relaton.org/healthz");
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: "ok", service: "relaton-events" });
  });

  it("publishes the AsyncAPI 3.x contract", async () => {
    const res = await SELF.fetch("https://events.relaton.org/v1/asyncapi.yaml");
    expect(res.status).toBe(200);
    const doc = await res.text();
    expect(doc).toContain("asyncapi: '3.0.0'");
    expect(doc).toContain("relaton/datasets/{flavor}/updated");
    expect(doc).toContain("relaton/records/{key}/changed");
    expect(doc).toContain("relaton/records/{key}/superseded");
    expect(doc).toContain("doc_count");
  });
});

describe("POST /admin/events", () => {
  it("requires the admin bearer token", async () => {
    const res = await SELF.fetch("https://events.relaton.org/admin/events", {
      method: "POST",
      body: JSON.stringify({ events: [] }),
    });
    expect(res.status).toBe(403);
  });

  it("rejects unknown types and invalid payloads", async () => {
    const bad1 = await SELF.fetch("https://events.relaton.org/admin/events", {
      method: "POST",
      headers: auth(),
      body: JSON.stringify({ events: [{ type: "flavor.exploded", flavor: "iso", payload: {} }] }),
    });
    expect(bad1.status).toBe(400);
    const bad2 = await SELF.fetch("https://events.relaton.org/admin/events", {
      method: "POST",
      headers: auth(),
      body: JSON.stringify({ events: [{ type: "record.changed", flavor: "iso", payload: { flavor: "iso" } }] }),
    });
    expect(bad2.status).toBe(400);
  });

  it("accepts typed events and lists them", async () => {
    const res = await SELF.fetch("https://events.relaton.org/admin/events", {
      method: "POST",
      headers: auth(),
      body: JSON.stringify({
        events: [
          { type: "dataset.updated", flavor: "iso", payload: { flavor: "iso", generation: "abc123", doc_count: 9247, manifest_sha: null } },
          { type: "record.changed", flavor: "iso", subject: "ISO 9001:2015", payload: { flavor: "iso", key: "ISO 9001:2015", reason: "updated" } },
          { type: "record.superseded", flavor: "iso", subject: "ISO 9001:2008", payload: { flavor: "iso", key: "ISO 9001:2008", reason: "superseded", superseded_by: "ISO 9001:2015" } },
        ],
      }),
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { accepted: number };
    expect(body.accepted).toBe(3);

    const list = await SELF.fetch("https://events.relaton.org/v1/events?flavor=iso");
    const { events } = (await list.json()) as { events: { type: string; payload: string }[] };
    expect(events.length).toBe(3);
    expect(events.map((e) => e.type)).toContain("record.superseded");
    const parsed = JSON.parse(events[0]!.payload) as Record<string, unknown>;
    expect(parsed.flavor).toBe("iso");
  });
});

describe("webhook delivery", () => {
  it("delivers a signed CloudEvents envelope and records the cursor", async () => {
    const secret = "test-secret-0123456789abcdef";
    const id = await insertEvent("dataset.updated", "rfc", "rfc", {
      flavor: "rfc",
      generation: "rfcgen001",
      doc_count: 3,
    });
    await DB.prepare("INSERT INTO subscriptions (url, secret, flavors) VALUES (?1, ?2, ?3)").bind(
      "https://consumer.example.com/hook",
      secret,
      "rfc",
    ).run();

    const captured: { url: string; init: RequestInit }[] = [];
    const stub = (async (url: string | URL, init?: RequestInit) => {
      captured.push({ url: String(url), init: init ?? {} });
      return new Response("ok", { status: 200 });
    }) as unknown as typeof fetch;

    const { attemptDelivery, hmacHex } = await import("../src/deliver");
    const { results } = await DB.prepare("SELECT * FROM events WHERE id = ?1").bind(id).all();
    const ev = results![0] as never;
    const { results: subs } = await DB.prepare("SELECT * FROM subscriptions WHERE url = ?1").bind("https://consumer.example.com/hook").all();
    await attemptDelivery(DB, ev, subs![0] as never, stub);

    expect(captured).toHaveLength(1);
    const headers = captured[0]!.init.headers as Record<string, string>;
    expect(headers["ce-specversion"]).toBe("1.0");
    expect(headers["ce-type"]).toBe("relaton.dataset.updated");
    expect(headers["ce-subject"]).toBe("rfc");
    expect(headers["webhook-signature"]).toMatch(/^v1,[0-9a-f]{64}$/);
    const expected = await hmacHex(secret, `${headers["webhook-id"]}.${headers["webhook-timestamp"]}.${captured[0]!.init.body}`);
    expect(headers["webhook-signature"]).toBe(`v1,${expected}`);

    const row = await DB.prepare("SELECT status, attempts FROM deliveries WHERE event_id = ?1").bind(id).first<{ status: string; attempts: number }>();
    expect(row?.status).toBe("delivered");
    expect(row?.attempts).toBe(1);
  });

  it("marks a delivery failed after a non-2xx and retries within bounds", async () => {
    const id = await insertEvent("record.changed", "iso", "ISO 1", { flavor: "iso", key: "ISO 1", reason: "created" });
    await DB.prepare("INSERT INTO subscriptions (url, secret, flavors) VALUES (?1, ?2, '*')").bind("https://down.example.com/hook", "s", ).run();
    const stub = (async () => new Response("nope", { status: 500 })) as unknown as typeof fetch;
    const { attemptDelivery } = await import("../src/deliver");
    const { results: evs } = await DB.prepare("SELECT * FROM events WHERE id = ?1").bind(id).all();
    const { results: subs } = await DB.prepare("SELECT * FROM subscriptions WHERE url = ?1").bind("https://down.example.com/hook").all();
    for (let i = 0; i < 5; i++) {
      await attemptDelivery(DB, evs![0] as never, subs![0] as never, stub);
    }
    const row = await DB.prepare("SELECT status, attempts FROM deliveries WHERE event_id = ?1").bind(id).first<{ status: string; attempts: number }>();
    expect(row?.status).toBe("failed");
    expect(row?.attempts).toBe(5);
  });

  it("creates and lists subscriptions without leaking secrets", async () => {
    const created = await SELF.fetch("https://events.relaton.org/admin/subscriptions", {
      method: "POST",
      headers: auth(),
      body: JSON.stringify({ url: "https://example.org/hook", flavors: "*" }),
    });
    expect(created.status).toBe(201);
    const { secret, id } = (await created.json()) as { secret: string; id: number };
    expect(secret).toMatch(/^[0-9a-f-]{30,}$/);

    const list = await SELF.fetch("https://events.relaton.org/admin/subscriptions", { headers: auth() });
    const body = (await list.json()) as { subscriptions: Record<string, unknown>[] };
    const sub = body.subscriptions.find((s) => s.id === id)!;
    expect(sub).not.toHaveProperty("secret");
    expect(sub.url).toBe("https://example.org/hook");
  });
});

describe("feeds", () => {
  it("serves Atom and JSON Feed per flavor", async () => {
    const atom = await SELF.fetch("https://events.relaton.org/v1/feeds/iso.atom");
    expect(atom.status).toBe(200);
    expect(atom.headers.get("content-type")).toContain("atom");
    const xml = await atom.text();
    expect(xml).toContain("<feed xmlns");
    expect(xml).toContain("dataset.updated");

    const json = await SELF.fetch("https://events.relaton.org/v1/feeds/iso.json");
    expect(json.status).toBe(200);
    const feed = (await json.json()) as { version: string; items: unknown[] };
    expect(feed.version).toContain("jsonfeed.org");
    expect(feed.items.length).toBeGreaterThan(0);
  });
});
