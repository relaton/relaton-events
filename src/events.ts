// The event types and their payload shapes — the runtime mirror of
// asyncapi.yaml (the contract document is the source of truth; this
// module carries the pieces the worker reads and writes).
export const EVENT_TYPES = ["dataset.updated", "record.changed", "record.superseded"] as const;
export type EventType = (typeof EVENT_TYPES)[number];

export interface DatasetUpdated {
  flavor: string;
  generation: string;
  doc_count: number;
  manifest_sha: string | null;
}

export interface RecordChanged {
  flavor: string;
  key: string;
  reason: "created" | "updated" | "superseded" | "deleted";
  superseded_by?: string | null;
}

export type EventPayload = DatasetUpdated | RecordChanged;

export interface EventRow {
  id: number;
  type: string;
  flavor: string;
  subject: string;
  payload: string;
  created_at: string;
}

// CloudEvents 1.0 envelope (the AsyncAPI document binds these headers):
// the delivery body is the typed payload; the envelope rides the headers.
export function eventHeaders(row: EventRow): Record<string, string> {
  const payload = JSON.parse(row.payload) as EventPayload;
  return {
    "ce-specversion": "1.0",
    "ce-type": `relaton.${row.type.replace(".", ".")}`,
    "ce-source": `https://events.relaton.org/${row.flavor}`,
    "ce-id": `relaton-events:${row.id}`,
    "ce-subject": row.subject,
    "ce-time": new Date(row.created_at + "Z").toISOString(),
    "content-type": "application/json",
  };
}

export function isEventType(t: string): t is EventType {
  return (EVENT_TYPES as readonly string[]).includes(t);
}

// Payload validation per type — the shapes asyncapi.yaml declares.
export function validatePayload(type: EventType, payload: unknown): string | null {
  const p = payload as Record<string, unknown>;
  if (type === "dataset.updated") {
    if (typeof p?.flavor !== "string" || !p.flavor) return "payload.flavor is required";
    if (typeof p?.generation !== "string" || !p.generation) return "payload.generation is required";
    if (typeof p?.doc_count !== "number") return "payload.doc_count must be a number";
    return null;
  }
  if (typeof p?.flavor !== "string" || !p.flavor) return "payload.flavor is required";
  if (typeof p?.key !== "string" || !p.key) return "payload.key is required";
  if (!["created", "updated", "superseded", "deleted"].includes(String(p?.reason))) {
    return "payload.reason must be created | updated | superseded | deleted";
  }
  if (type === "record.superseded" && p?.reason !== "superseded") {
    return "record.superseded requires reason=superseded";
  }
  return null;
}
