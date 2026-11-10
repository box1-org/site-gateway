# site-gateway

Edge shipment applications

A code-only Quarkus multi-module repository created from the Box1 `quarkus-monorepo`
golden path. Nothing here is deployed to OpenShift: the modules are packaged into an
OCI bundle that **site-gateway-os** copies into its image by digest.

## Modules

| Module | Directory | In the bundle | Port (127.0.0.1) | What it does |
| --- | --- | --- | --- | --- |
| `gateway` | [`gateway/`](gateway/) | `/gateway/quarkus-app/` | 8080 | MQTT → outbox → Kafka forwarder with a bounded disk buffer, pick-list consumer, local API for site-ops, site status heartbeat to asset-service (box1-docs#127, #129, #130) |
| `ops` | [`ops/`](ops/) | `/ops/quarkus-app/` | 8081 | site-ops handheld pages (Qute + htmx): pick confirmation, cycle counts, holds (box1-docs#129) |
| `simulator` | [`simulator/`](simulator/) | `/simulator/quarkus-app/` | 8082 | Seeded equipment simulator: the site's fixture asset over MQTT every 5 s, with a breach scenario (box1-docs#127, #128) |

Each module is a Quarkus application built as a fast-jar (`target/quarkus-app/`,
started with `java -jar quarkus-app/quarkus-run.jar`). Design, decisions and data are in
[`docs/architecture.md`](docs/architecture.md); operations (buffer, drain, replay, overflow, site-ops access,
breach scenario) in [`docs/runbook.md`](docs/runbook.md). Contracts: `box1-ops-contracts` `SITE.md` at the commit
pinned by `contracts.ref` in `pom.xml`.

## What the site image must provide

`site-gateway-os` runs the modules as user `box1-site` (box1-docs#140) with these settings. Everything not listed
has a default in the module's `application.properties`.

| Setting | Source | Used by |
| --- | --- | --- |
| `BOX1_SITE_ID`, `BOX1_INSTANCE` | `/etc/box1/site.env` (cloud-init) | all; startup fails without `BOX1_SITE_ID` |
| `BOX1_KEYCLOAK_TOKEN_URL`, `BOX1_KEYCLOAK_CLIENT_ID`, `BOX1_KEYCLOAK_CLIENT_SECRET_FILE` | `/etc/box1/site.env` | gateway heartbeat; off when absent |
| `BOX1_ASSET_SERVICE_URL`, `BOX1_KAFKA_BOOTSTRAP_SERVERS`, `BOX1_OTLP_ENDPOINT` | `/etc/box1/site.env`, optional | defaults `https://asset-service-dev.apps.ocp.box1.org`, `box1-kafka-kafka-bootstrap.box1-kafka.svc:9092`, `http://box1-collector.box1-observability.svc:4317` |
| `SITE_DB_PASSWORD` (and optionally `SITE_DB_URL`, `SITE_DB_USER`; defaults `jdbc:postgresql://127.0.0.1:5432/site`, `site`) | an environment file generated at first boot, never in Git or the image | gateway |
| `HTTP_PORT` | the unit: 8080 gateway, 8081 ops, 8082 simulator | all |
| MQTT on `tcp://127.0.0.1:1883`, anonymous, loopback only | AMQ Broker quadlet | gateway, simulator |
| `/run/box1/site-facts.json`: `bootedImageDigest`, `stagedImageDigest`, `rollbackImageDigest` (`sha256:…` or null), `greenbootStatus` (`GREEN`/`RED`/`UNKNOWN`), `bundleDigest` | a root service in the image | gateway heartbeat; missing or malformed values are sent as null / `UNKNOWN` |

greenboot required checks use `http://127.0.0.1:8080/q/health/ready` (PostgreSQL reachable, outbox migrations
applied, MQTT subscription connected; never Kafka) and `http://127.0.0.1:8081/q/health/ready`.

## Build and test

```sh
./mvnw verify            # every module: unit and Quarkus tests, Checkstyle, SpotBugs/FindSecBugs
./mvnw -pl <module> quarkus:dev
# The containers suite (PostgreSQL, Kafka, ActiveMQ Artemis MQTT via Testcontainers): MQTT to Kafka end to end,
# duplicates, Kafka and database outages, overflow, backpressure, crash between acknowledgement and commit, pick list.
DOCKER_HOST=<docker or podman socket> TESTCONTAINERS_RYUK_DISABLED=true ./mvnw -pl gateway verify -Pcontainers
```

Contract tests fetch the schemas from `box1-ops-contracts` at `contracts.ref`, so they need GitHub access.

Java 21 and the Quarkus platform version are set once in the parent `pom.xml`.

## Delivery

- **Pull requests** run the multi-module `mvn verify` (`.tekton/pull-request.yaml`,
  Task `box1-maven-bundle` without publishing). Keep it a required check.
- **Merges to `main`** (`.tekton/release.yaml`, run as `site-gateway-release`):
  1. `box1-maven-bundle` verifies again and pushes a deterministic `FROM scratch` image to
     `box1-ci/site-gateway-bundle:<revision>`, by digest;
  2. `box1-security-gates` scans it in `gate` mode (SBOM to RHTPA, RHACS check);
  3. only if the gate passes, `box1-bundle-consumer-pr` opens or updates one PR in
     `box1-org/site-gateway-os` that changes the digest on the single line
     `COPY --from=…/box1-ci/site-gateway-bundle@sha256:<digest> / <path>` in
     `Containerfile`. It never merges.

The consumer file must contain exactly one such line, for example:

```dockerfile
COPY --from=image-registry.openshift-image-registry.svc:5000/box1-ci/site-gateway-bundle@sha256:<digest> / /usr/lib/site-gateway/
```

The CI configuration (PaC Repository, release ServiceAccount, bundle ImageStream and
its push permission) lives in
[`box1-workloads-dev/ci/site-gateway`](https://github.com/box1-org/box1-workloads-dev/tree/main/ci/site-gateway).
To change the consumer, change `consumer-repository`/`consumer-path` in
`.tekton/release.yaml` **and** the Repository's `github_app_token_scope_repos` there.

Reposilite is a dependency cache only; no JARs are published to it.
