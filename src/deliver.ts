// Webhook delivery: HMAC-signed (secp256k1-free — HMAC-SHA256 over the
// raw body with the subscription's secret), idempotent per (event,
// subscription) via the deliveries table, bounded retries with backoff.
// Deliveries fan out from ctx.waitUntil — the POST that records the
// events never blocks on subscriber round-trips.
import { eventHeaders, type EventRow } from "./events";

const MAX_ATTEMPTS = 5;
const TIMEOUT_MS = 10_000;

export interface SubscriptionRow {
  id: number;
  url: string;
  secret: string;
  flavors: string;
  active: number;
}

export async function hmacHex(secret: string, body: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(body));
  return [...new Uint8Array(sig)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function wants(row: SubscriptionRow, flavor: string): boolean {
  return row.flavors === "*" || row.flavors.split(",").map((s) => s.trim()).includes(flavor);
}

// Record the delivery cursors for every active subscriber, then deliver
// pending ones within the ingest budget: one attempt now per cursor,
// backoff rows left pending for the retry sweep.
export async function dispatchEvents(
  db: D1Database,
  events: EventRow[],
  waitUntil: (p: Promise<unknown>) => void,
  fetcher: typeof fetch = fetch,
): Promise<void> {
  const { results } = await db.prepare("SELECT * FROM subscriptions WHERE active = 1").all<SubscriptionRow>();
  const subs = results ?? [];
  if (subs.length === 0) return;

  for (const ev of events) {
    const matching = subs.filter((s) => wants(s, ev.flavor));
    if (matching.length === 0) continue;
    await db.batch(
      matching.map((s) =>
        db.prepare("INSERT OR IGNORE INTO deliveries (event_id, subscription_id) VALUES (?1, ?2)").bind(ev.id, s.id),
      ),
    );
    for (const s of matching) {
      waitUntil(attemptDelivery(db, ev, s, fetcher));
    }
  }
}

// `fetcher` is injectable so tests can capture the signed delivery without
// network; production uses the global fetch.
export async function attemptDelivery(
  db: D1Database,
  ev: EventRow,
  sub: SubscriptionRow,
  fetcher: typeof fetch = fetch,
): Promise<void> {
  const cur = await db
    .prepare("SELECT attempts FROM deliveries WHERE event_id = ?1 AND subscription_id = ?2")
    .bind(ev.id, sub.id)
    .first<{ attempts: number }>();
  const attempts = (cur?.attempts ?? 0) + 1;

  const body = ev.payload;
  const headers: Record<string, string> = {
    ...eventHeaders(ev),
    "webhook-id": `${ev.id}-${sub.id}`,
    "webhook-timestamp": Math.floor(Date.now() / 1000).toString(),
  };
  headers["webhook-signature"] = `v1,${await hmacHex(sub.secret, `${headers["webhook-id"]}.${headers["webhook-timestamp"]}.${body}`)}`;

  let status: "delivered" | "failed" = "failed";
  try {
    const res = await fetcher(sub.url, {
      method: "POST",
      headers,
      body,
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (res.ok) status = "delivered";
  } catch {
    // timeout / network: a failed attempt
  }

  const done = status === "delivered" || attempts >= MAX_ATTEMPTS;
  await db
    .prepare(`INSERT INTO deliveries (event_id, subscription_id, status, attempts, last_attempt)
              VALUES (?1, ?2, ?3, ?4, datetime('now'))
              ON CONFLICT(event_id, subscription_id)
              DO UPDATE SET status = ?3, attempts = ?4, last_attempt = datetime('now')`)
    .bind(ev.id, sub.id, done ? status : "pending", attempts)
    .run();
}

// The retry sweep: pending deliveries older than their backoff window get
// another attempt. Called from a cron trigger.
export async function retryPending(db: D1Database, waitUntil: (p: Promise<unknown>) => void): Promise<number> {
  const { results } = await db
    .prepare(`SELECT e.*, s.id AS sub_id, s.url, s.secret, s.flavors, s.active
              FROM deliveries d
              JOIN events e ON e.id = d.event_id
              JOIN subscriptions s ON s.id = d.subscription_id
              WHERE d.status = 'pending' AND d.attempts < 5
                AND (d.last_attempt IS NULL OR datetime(d.last_attempt, '+1 minute') < datetime('now'))`)
    .all<EventRow & { sub_id: number; url: string; secret: string; flavors: string; active: number }>();
  const rows = results ?? [];
  for (const r of rows) {
    waitUntil(attemptDelivery(db, r, { id: r.sub_id, url: r.url, secret: r.secret, flavors: r.flavors, active: r.active }));
  }
  return rows.length;
}
