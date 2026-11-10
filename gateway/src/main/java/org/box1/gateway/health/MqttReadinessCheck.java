package org.box1.gateway.health;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.component.paho.PahoConsumer;
import org.box1.gateway.config.SiteConfig;
import org.box1.gateway.ingest.ReadingRoute;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

/** Ready only when the durable MQTT subscription to the local AMQ Broker is connected (box1-docs#127, #133). */
@Readiness
@ApplicationScoped
public class MqttReadinessCheck implements HealthCheck {
    private final CamelContext camel;
    private final SiteConfig config;

    public MqttReadinessCheck(CamelContext camel, SiteConfig config) {
        this.camel = camel;
        this.config = config;
    }

    @Override
    public HealthCheckResponse call() {
        if (!config.mqtt().enabled()) {
            return HealthCheckResponse.named("site-mqtt").up().withData("check", "disabled").build();
        }
        Route route = camel.getRoute(ReadingRoute.ROUTE_ID);
        boolean connected = route != null && route.getConsumer() instanceof PahoConsumer consumer
                && consumer.getClient() != null && consumer.getClient().isConnected();
        return HealthCheckResponse.named("site-mqtt").status(connected)
                .withData("broker", config.mqtt().brokerUrl()).build();
    }
}
