# Architecture

site-gateway is the application half of an edge site (box1-docs#126). Each site VM (`site-west` = `site_ds9`,
`site-east` = `site_spacedock`) runs the three modules under systemd from the `box1-org/site-gateway-os` image, next
to a local AMQ Broker (MQTT) and a local PostgreSQL, both digest-pinned Podman quadlets. The contracts are in
`box1-ops-contracts` (`SITE.md`, `TELEMETRY.md`, `events/v1/site.*`, the asset-service site status API).

```text
            site VM (RHEL 10 image mode)                                         cluster
 ┌───────────────────────────────────────────────────────────────┐
 │ simulator ──MQTT QoS1──► AMQ Broker ──durable sub──► gateway   │
 │  :8082 (local)          127.0.0.1:1883                :8080    │   box1.telemetry.events.v1 (key assetId)
 │                                                     │  outbox ─┼─► box1.site.events.v1 (key siteId)
 │ site-ops :8081 ──local API──────────────────────────►│ (PG)    │
 │  (Qute + htmx)                                       │         │◄─ box1.inventory.events.v1, box1.fulfillment.events.v1
 │                                                      │ pick list│     (group site-gateway-<siteId>)
 │                                                      └─ agent ─┼─► PUT asset-service /api/v1/sites/{siteId}/status
 │                                                                │   (Keycloak client credentials, HTTPS)
 │ box1-site-facts (root) ─► /run/box1/site-facts.json ─► agent   │
 └───────────────────────────────────────────────────────────────┘   OTLP traces ─► box1-collector
```

## Modules

| Module | Port (loopback) | Role | Issues |
|---|---|---|---|
| `simulator` | 8082 | Seeded, deterministic readings of the site's fixture asset every 5 s over MQTT; a bounded breach scenario | #127, #128 |
| `gateway` | 8080 | MQTT → outbox forwarder (Camel Quarkus), outbox publisher, pick-list consumer, local API, site agent heartbeat, readiness | #127, #129, #130, #133 |
| `ops` | 8081 | site-ops handheld pages (Qute + htmx): pick confirmation, cycle counts, holds | #129 |

The gateway owns the site database schema (Flyway, `gateway/src/main/resources/db/migration`). site-ops has no
state: every action goes to the gateway's local API, which writes the local effect and its site event to the outbox
in one transaction.

## Decisions

| Topic | Decision | Why |
|---|---|---|
| MQTT | Topic `box1/sites/<siteId>/readings`, QoS 1. The gateway subscribes durably (client ID `site-gateway-<siteId>`, clean session off) and acknowledges a message only after its outbox row is committed (`manualAcksEnabled`). A database outage blocks the subscription with backoff; at shutdown the message stays unacknowledged and the broker redelivers it | At-least-once from broker to outbox; the outbox insert is idempotent on `eventId` |
| Reading `eventId` | `UUID.nameUUIDFromBytes("box1/equipment.reading.v1/<assetId>/<metricName>/<measuredAt>")`, as asset-service and `TELEMETRY.md`; a test checks the contract's test vector | A re-sent reading is the same event downstream |
| Reading key and topic | `box1.telemetry.events.v1`, key `assetId` (the readings contract), `causationId` `ingest:site`. The issue's "keyed by `siteId`" applies to site events only | Changing the readings key would be a breaking contract change |
| Site events | `box1.site.events.v1`, key and `aggregateId` `siteId`; random `eventId` generated with the local action; site-local IDs `pick_…`, `cnt_…`, `hold_…`; header `box1-site-id` | SITE.md |
| Outbox | One table, single publisher, rows locked `FOR UPDATE SKIP LOCKED` in `seq` order; a row is marked published only after the broker acknowledged it, as the acknowledged prefix of each batch | Ordered per site and per asset; a crash re-sends with the same `eventId` |
| Producer | `acks=all`, idempotence on, 5 in flight, `delivery.timeout.ms` 30 s, `request.timeout.ms` 10 s, `max.block.ms` 10 s; recreated after any failure; backoff 1 s doubling to 60 s | Bounded failure while disconnected, no busy loop |
| Buffer bound | Readings: at most 100 000 unpublished and at most 12 h old. Site events: at most 20 000 unpublished. Published rows kept 48 h | At 1 reading / 5 s / asset a site produces about 17 300 readings a day; 100 000 rows is under 100 MB; asset-service rejects readings older than its 24 h retention |
| Overflow | Readings: the oldest unpublished are dropped first, each drop recorded in `outbox_drop` and counted. Site events: never dropped; a new operator action is refused with 503 `BUFFER_FULL` and nothing is recorded | Operator actions are business data; readings are telemetry |
| Pick list | Group `site-gateway-<siteId>` on `box1.inventory.events.v1` and `box1.fulfillment.events.v1`, `earliest`; keeps `inventory.reserved.v1` for its own site; `inventory.reservation-expired.v1` closes an open item; `shipment.confirmed.v1` closes any item; a closure that arrives first is remembered (`pick_closure`); offsets committed after the local commit | SITE.md; works offline; replay-safe |
| Pick | Confirms the whole reserved quantity of one open item; no partial picks in v1 | Matches `site.pick-confirmed.v1` and inventory-service's rule |
| Status | Every 15 s; not buffered; facts from `/run/box1/site-facts.json` so the gateway needs no privilege | SITE.md, box1-docs#130 |
| Readiness | Gateway ready only when PostgreSQL answers, every packaged migration is applied, and the MQTT subscription is connected. Kafka and asset-service are not checked | greenboot required checks (box1-docs#133) must not roll back a healthy site whose link is down |
| Local only | All three modules listen on 127.0.0.1 | The pages have no login; reach them through a tunnel |
| Traces | Simulator starts a trace per reading (`traceparent` in the MQTT message); the gateway continues it and stores the `traceparent` in the envelope, so asset-service continues it; OTLP to `box1-collector` | box1-docs#132 |
| Logs | JSON to journald with `siteId`, `service`, and the OpenTelemetry MDC (`traceId`, `spanId`); no secret is logged | box1-docs#132 |

## Data

The site PostgreSQL holds `outbox`, `outbox_drop`, `pick_item`, `pick_closure`, `site_hold` and `site_action`
(`V1__site_schema.sql`). Its data directory is under `/var` in the VM, which survives reboots, `bootc upgrade` and
`bootc rollback`. Because a rollback runs the previous release on the current schema, later migrations must stay
backward compatible (additive only).

## Security

- No credential is in this repository, the bundle or the image. The Keycloak client secret is read from the file
  cloud-init writes (`BOX1_KEYCLOAK_CLIENT_SECRET_FILE`, readable only by `box1-site`) at each token request, so a
  rotated secret applies without a restart. The database password comes from an environment file generated at
  first boot by the image (`SITE_DB_PASSWORD`).
- A site forwards only readings whose `siteId` is its own.
- Until box1-docs#71, Kafka has no client authentication; the `box1-site-id` header is informational.
