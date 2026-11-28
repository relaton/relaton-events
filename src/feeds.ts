// Atom + JSON Feed rendering of the event log, per flavor.
import type { EventRow } from "./events";

function esc(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function iso(row: EventRow): string {
  return new Date(row.created_at + "Z").toISOString();
}

export function atomFeed(base: string, flavor: string, rows: EventRow[]): string {
  const entries = rows
    .map((r) => {
      const payload = JSON.parse(r.payload) as Record<string, unknown>;
      const title = `${r.type}: ${r.subject}`;
      const summary =
        r.type === "dataset.updated"
          ? `${flavor} dataset updated (generation ${String(payload.generation)}, ${String(payload.doc_count)} documents)`
          : `${String(payload.key)} ${String(payload.reason)}${payload.superseded_by ? ` — superseded by ${String(payload.superseded_by)}` : ""}`;
      return `  <entry>
    <id>urn:relaton-events:${r.id}</id>
    <title>${esc(title)}</title>
    <updated>${iso(r)}</updated>
    <link rel="alternate" href="${esc(base)}/v1/events/${r.id}"/>
    <summary>${esc(summary)}</summary>
  </entry>`;
    })
    .join("\n");
  return `<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Relaton Index events — ${esc(flavor)}</title>
  <id>${esc(base)}/v1/feeds/${esc(flavor)}.atom</id>
  <updated>${rows[0] ? iso(rows[0]) : new Date().toISOString()}</updated>
  <link rel="self" href="${esc(base)}/v1/feeds/${esc(flavor)}.atom"/>
${entries}
</feed>`;
}

// JSON Feed 1.1 (jsonfeed.org)
export function jsonFeed(base: string, flavor: string, rows: EventRow[]): string {
  return JSON.stringify(
    {
      version: "https://jsonfeed.org/version/1.1",
      title: `Relaton Index events — ${flavor}`,
      feed_url: `${base}/v1/feeds/${flavor}.json`,
      home_page_url: "https://www.relaton.org",
      items: rows.map((r) => {
        const payload = JSON.parse(r.payload) as Record<string, unknown>;
        return {
          id: `urn:relaton-events:${r.id}`,
          url: `${base}/v1/events/${r.id}`,
          date_published: iso(r),
          title: `${r.type}: ${r.subject}`,
          content_text:
            r.type === "dataset.updated"
              ? `${flavor} dataset updated (generation ${String(payload.generation)}, ${String(payload.doc_count)} documents)`
              : `${String(payload.key)} ${String(payload.reason)}`,
        };
      }),
    },
    null,
    2,
  );
}
