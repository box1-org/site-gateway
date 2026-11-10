# site-gateway

Edge shipment applications

A code-only Quarkus multi-module repository created from the Box1 `quarkus-monorepo`
golden path. Nothing here is deployed to OpenShift: the modules are packaged into an
OCI bundle that **site-gateway-os** copies into its image by digest.

## Modules

| Module | Directory | In the bundle |
| --- | --- | --- |
| `gateway` | [`gateway/`](gateway/) | `/gateway/quarkus-app/` |
| `ops` | [`ops/`](ops/) | `/ops/quarkus-app/` |
| `simulator` | [`simulator/`](simulator/) | `/simulator/quarkus-app/` |

Each module is a Quarkus application built as a fast-jar (`target/quarkus-app/`,
started with `java -jar quarkus-app/quarkus-run.jar`).

## Build and test

```sh
./mvnw verify            # every module: unit and Quarkus tests, Checkstyle, SpotBugs/FindSecBugs
./mvnw -pl <module> quarkus:dev
```

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
