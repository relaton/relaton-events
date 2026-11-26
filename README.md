# relaton-events

Relaton event plane — the freshness notification surface of the Relaton Index:
AsyncAPI-specified webhooks, SSE, and Atom/JSON feeds.

Epic: relaton/relaton-api-js#175 (Week 2). Implementation charter lives in
this repo's issue tracker.

## What this service does

- **Webhook dispatch** for typed events:
  - `relaton.datasets.{flavor}.updated`
  - `relaton.records.{key}.changed`
  - supersession events
- **SSE channel** for live consumers
- **Atom + JSON Feed per flavor**; sitemap generation support for www
- Triggered by Deploy-completion (the existing ingest re-dispatch pattern),
  delivering within the ingest budget

## Contract

The event surface is specified as an **AsyncAPI 3.x document**. Typed payload
shapes are generated from LML-derived schemas — never hand-mirrored. The
AsyncAPI document is the source of truth for consumers; generated typed
clients fall out of it.

## Cloudflare deployment

- Worker: `events-relaton-org` (`wrangler.jsonc`)
- Route: `events.relaton.org` (custom domain, enable once zone confirmed)
- D1 database `events-relaton-org`: subscription registry + delivery cursors
  (create first, then uncomment the binding with its id)
- GitHub secrets: `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`
- Dispatch trigger: Deploy-completion workflow in relaton-api-js

## Laws (binding)

- LML-first SSOT: payload shapes derive from relaton-models; no hand-rolled
  serialization.
- Delivery is idempotent and retry-safe; no event storm on re-ingest
  (manifest-sha gating already dedupes at the source).
- No daily full-corpus sync — incremental, budget-shaped only.

## Development

```sh
npm install
npm run dev      # local workerd
npm test         # vitest
npm run deploy   # requires CF secrets
```
