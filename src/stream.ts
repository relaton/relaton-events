// The live SSE channel: replays the log tail from the client's Last-Event-ID
// (or the newest row), then polls the D1 tail every 5s inside the stream.
// One cursor per connection; the stream ends when the client disconnects.
import type { Env } from "./env";
import type { EventRow } from "./events";

const POLL_MS = 5000;
const TAIL_LIMIT = 50;

async function tail(db: D1Database, afterId: number, flavor: string | null): Promise<EventRow[]> {
  const stmt = flavor
    ? db.prepare("SELECT * FROM events WHERE id > ?1 AND flavor = ?2 ORDER BY id LIMIT ?3").bind(afterId, flavor, TAIL_LIMIT)
    : db.prepare("SELECT * FROM events WHERE id > ?1 ORDER BY id LIMIT ?2").bind(afterId, TAIL_LIMIT);
  const { results } = await stmt.all<EventRow>();
  return results ?? [];
}

export function sseStream(request: Request, db: D1Database, flavor: string | null): Response {
  const encoder = new TextEncoder();
  const lastEventId = request.headers.get("last-event-id");
  let cursor: number | null = lastEventId ? Number(lastEventId) : null;

  const stream = new ReadableStream({
    async start(controller) {
      let closed = false;
      const push = (chunk: string) => {
        if (!closed) controller.enqueue(encoder.encode(chunk));
      };
      request.signal.addEventListener("abort", () => {
        closed = true;
        try { controller.close(); } catch { /* already closed */ }
      });
      push(": relaton-events live stream\n\n");

      // First frame: the current newest id, so fresh clients start from now
      // (or replay from their Last-Event-ID).
      const newest = await db.prepare("SELECT COALESCE(MAX(id), 0) AS n FROM events").first<{ n: number }>();
      if (cursor === null) {
        cursor = newest?.n ?? 0;
        push(`retry: ${POLL_MS}\n\n`);
      }

      while (!closed) {
        try {
          const rows = cursor !== null ? await tail(db, cursor, flavor) : [];
          for (const row of rows) {
            push(`id: ${row.id}\nevent: ${row.type}\ndata: ${row.payload}\n\n`);
            cursor = Math.max(cursor, row.id);
          }
        } catch {
          push(": tail read failed\n\n");
        }
        await new Promise((r) => setTimeout(r, POLL_MS));
      }
    },
  });

  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
    },
  });
}
