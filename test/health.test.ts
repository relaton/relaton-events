import { describe, expect, it } from "vitest";
import { SELF } from "cloudflare:test";

describe("health", () => {
  it("reports ok", async () => {
    const res = await SELF.fetch("https://events.relaton.org/healthz");
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ status: "ok", service: "relaton-events" });
  });
});