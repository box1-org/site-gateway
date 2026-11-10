package org.box1.gateway.it;

import java.util.List;
import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

/** Everything on except the heartbeat: MQTT route, publisher, pick-list consumer, scheduler, migrations. */
public class LiveProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.ofEntries(Map.entry("site.mqtt.enabled", "true"), Map.entry("site.kafka.publisher-enabled", "true"),
                Map.entry("site.kafka.pick-list-enabled", "true"), Map.entry("quarkus.scheduler.enabled", "true"),
                Map.entry("quarkus.flyway.migrate-at-start", "true"), Map.entry("gateway.readiness.database", "true"),
                Map.entry("gateway.publisher.interval", "250ms"), Map.entry("gateway.buffer.stats-interval", "1s"));
    }

    @Override
    public List<TestResourceEntry> testResources() {
        return List.of(new TestResourceEntry(SiteContainers.class));
    }

    @Override
    public boolean disableGlobalTestResources() {
        return true;
    }
}
