package org.box1.gateway.config;

import java.time.Duration;
import java.util.Optional;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/**
 * Site configuration. The identity comes from /etc/box1/site.env (written by cloud-init from the instance's
 * {@code <instance>-site} ConfigMap, box1-docs#140), which systemd passes to the service as environment variables.
 */
@ConfigMapping(prefix = "site")
public interface SiteConfig {
    /** The fixture site this VM is, for example site_ds9 (BOX1_SITE_ID). Startup fails without it. */
    String id();

    /** The VM instance name, for example site-west (BOX1_INSTANCE). */
    Optional<String> instance();

    Mqtt mqtt();

    Kafka kafka();

    Buffer buffer();

    Agent agent();

    interface Mqtt {
        @WithDefault("tcp://127.0.0.1:1883")
        String brokerUrl();

        /** Readings arrive on box1/sites/<siteId>/readings. */
        @WithDefault("true")
        boolean enabled();
    }

    interface Kafka {
        String bootstrapServers();

        @WithDefault("box1.telemetry.events.v1")
        String readingsTopic();

        @WithDefault("box1.site.events.v1")
        String siteTopic();

        @WithDefault("box1.inventory.events.v1")
        String inventoryTopic();

        @WithDefault("box1.fulfillment.events.v1")
        String fulfillmentTopic();

        /** Runs the pick-list consumer (group site-gateway-<siteId>). */
        @WithDefault("true")
        boolean pickListEnabled();

        /** Runs the outbox publisher. */
        @WithDefault("true")
        boolean publisherEnabled();
    }

    /** The bounded outbox (box1-docs#127, NFR-06). */
    interface Buffer {
        /** Unpublished readings kept at most; the oldest are dropped first when a new one arrives. */
        @WithDefault("100000")
        int maxReadings();

        /** Unpublished readings older than this are dropped (asset-service rejects readings older than 24 h). */
        @WithDefault("PT12H")
        Duration maxReadingAge();

        /** Unpublished site events kept at most. Never dropped: new operator actions are refused instead. */
        @WithDefault("20000")
        int maxSiteEvents();

        /** How long published rows are kept for site-side counts before they are pruned. */
        @WithDefault("PT48H")
        Duration publishedRetention();

        @WithDefault("500")
        int publishBatch();
    }

    /** The site agent's heartbeat to asset-service (box1-docs#130). */
    interface Agent {
        @WithDefault("true")
        boolean enabled();

        @WithDefault("https://asset-service-dev.apps.ocp.box1.org")
        String assetServiceUrl();

        Optional<String> tokenUrl();

        Optional<String> clientId();

        /** File holding the site's Keycloak client secret, readable only by box1-site (cloud-init, box1-docs#140). */
        Optional<String> clientSecretFile();

        /** OS facts (booted digest, greenboot) written by site-gateway-os's box1-site-facts.service. */
        @WithDefault("/run/box1/site-facts.json")
        String factsFile();
    }
}
