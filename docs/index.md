# site-gateway

Edge shipment applications

Code-only Quarkus modules, delivered as the OCI bundle `box1-ci/site-gateway-bundle`
and consumed by `box1-org/site-gateway-os` (`Containerfile`).
See the repository README for the build, the release pipeline and the bundle layout.

## Bundle layout

```text
/gateway/quarkus-app/      java -jar /gateway/quarkus-app/quarkus-run.jar
/ops/quarkus-app/      java -jar /ops/quarkus-app/quarkus-run.jar
/simulator/quarkus-app/      java -jar /simulator/quarkus-app/quarkus-run.jar
/SHA256SUMS                      sha256 of every file above
```
