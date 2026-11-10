# Runbook

Commands run inside a site VM (`virtctl ssh cloud-user@vmi/site-west -n site-west-dev`, then `sudo -i` where noted).
The site database is reachable from the VM as `psql -h 127.0.0.1 -U site site` with the password in the environment
file named by the `site-gateway` unit (`systemctl cat site-gateway`). Never copy that password into a ticket or log.

## Service health

```sh
systemctl status site-gateway site-ops site-simulator
curl -s http://127.0.0.1:8080/q/health/ready     # site-database (migrations applied), site-mqtt
curl -s http://127.0.0.1:8080/api/v1/site/buffer
curl -s http://127.0.0.1:8082/api/v1/simulator
journalctl -u site-gateway -o cat -n 50
```

The gateway is not ready when PostgreSQL is down, a migration is pending or failed, or the MQTT subscription is not
connected. It stays ready when Kafka or asset-service is unreachable.

## Buffer depth and a clean drain

```sql
-- pending by class, and the age of the oldest pending row
SELECT event_class, count(*), now() - min(created_at) AS oldest
FROM outbox WHERE published_at IS NULL GROUP BY event_class;
-- drops caused by the bound
SELECT reason, sum(rows_dropped) FROM outbox_drop GROUP BY reason;
```

A drain is clean when `pending` reaches 0 and, for a window, recorded = published + dropped:

```sql
-- site-side counts for a window (published rows are kept 48 h)
SELECT event_class, count(*) AS recorded, count(published_at) AS published, count(DISTINCT event_id) AS distinct_ids
FROM outbox WHERE created_at >= '<start>' AND created_at < '<end>' GROUP BY event_class;
```

Compare with Kafka for the same window by `eventId` (distinct and total). A duplicate on Kafka has the same
`eventId` and is harmless: asset-service deduplicates readings on the natural key, inventory-service on `eventId`.

Metrics on `http://127.0.0.1:8080/q/metrics`, each with label `site_id`:

| Metric | Meaning |
|---|---|
| `site_outbox_depth{class="reading"\|"site_event"}` | Pending rows |
| `site_outbox_oldest_age_seconds` | Age of the oldest pending row |
| `site_outbox_recorded_total{class}` | Rows recorded since start |
| `site_outbox_published_total{class}` | Rows published since start |
| `site_outbox_dropped_total{reason="size"\|"age"}` | Readings dropped by the bound since start |
| `site_outbox_refused_total` | Operator actions refused because the site-event backlog was full |
| `site_readings_rejected_total{reason}` | MQTT messages that could never be valid readings |
| `site_picklist_records_total{outcome}` | Pick-list records by outcome (`added`, `duplicate`, `other_site`, `expired`, `shipped`, `ignored`, `poison`, `rejected`) |
| `site_heartbeat_total{outcome}` | Heartbeats by outcome (`ok`, `http_<status>`, `unreachable`, `unconfigured`) |

## Replay

To replay an already-published window to Kafka (for example, to prove a replay creates no duplicates downstream),
mark its rows unpublished. This re-sends identical envelopes with the same `eventId`s. It changes shared downstream
state only through those idempotent consumers, but it is still a fault injection on the cluster: get approval first.

```sql
UPDATE outbox SET published_at = NULL WHERE created_at >= '<start>' AND created_at < '<end>';
```

Rows already pruned (published more than 48 h ago) cannot be replayed from the site.

## Overflow

When the readings bound is reached, the oldest pending readings are dropped (`outbox_drop`, reason `SIZE`), and
pending readings measured more than 12 h ago are dropped every 30 s (reason `AGE`). When the site-event backlog reaches 20 000,
site-ops shows "The site buffer is full: nothing was recorded" and the gateway answers 503 `BUFFER_FULL`; nothing is
lost, the operator retries after the link returns. Limits are set with `SITE_BUFFER_MAX_READINGS`,
`SITE_BUFFER_MAX_READING_AGE`, `SITE_BUFFER_MAX_SITE_EVENTS` in the unit environment.

## Using site-ops

The pages listen on 127.0.0.1:8081 in the VM. From a workstation:

```sh
virtctl port-forward vmi/site-west -n site-west-dev 18081:8081   # then open http://127.0.0.1:18081/
```

## Breach scenario

```sh
curl -s -X POST -H 'Content-Type: application/json' -d '{"ticks":6}' http://127.0.0.1:8082/api/v1/simulator/breach
```

The next 6 readings of the site's asset carry its breach value (vibration 12.5 mm/s at `site_ds9`, temperature
98 C at `site_spacedock`, above asset-service's critical thresholds), then values return below the clear level, so
asset-service raises one alert and clears it.

## Restarts and rollback

`systemctl restart site-gateway` is safe at any time: unacknowledged MQTT messages are redelivered, uncommitted
outbox rows were never marked published, and the pick-list consumer resumes from committed offsets. A release is
rolled back by booting the previous OS deployment (`bootc rollback`, or greenboot automatically); the outbox
schema stays, so migrations must be additive.
