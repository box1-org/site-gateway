package org.box1.gateway.ingest;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;
import org.box1.gateway.config.SiteConfig;

/**
 * MQTT → outbox (box1-docs#127). A durable subscription (clean session off) at QoS 1 on the local AMQ Broker, with
 * manual acknowledgement: a message is acknowledged only when the route completes, after its outbox row is committed.
 * While the gateway is down the broker keeps the messages for this client ID.
 */
@ApplicationScoped
public class ReadingRoute extends RouteBuilder {
    public static final String ROUTE_ID = "mqtt-readings";

    private final SiteConfig config;
    private final ReadingIngest ingest;

    public ReadingRoute(SiteConfig config, ReadingIngest ingest) {
        this.config = config;
        this.ingest = ingest;
    }

    public static String topic(String siteId) {
        return "box1/sites/" + siteId + "/readings";
    }

    @Override
    public void configure() {
        if (!config.mqtt().enabled()) {
            return;
        }
        fromF("paho:%s?brokerUrl=%s&clientId=site-gateway-%s&qos=1&cleanSession=false&manualAcksEnabled=true"
                + "&automaticReconnect=true&connectionTimeout=10&keepAliveInterval=30&persistence=MEMORY",
                topic(config.id()), config.mqtt().brokerUrl(), config.id())
                .routeId(ROUTE_ID)
                .process(exchange -> ingest.accept(exchange.getMessage().getBody(byte[].class)));
    }
}
