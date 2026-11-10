package org.box1.simulator;

import java.util.List;
import java.util.Optional;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

/** Simulator configuration. The site comes from /etc/box1/site.env (BOX1_SITE_ID). */
@ConfigMapping(prefix = "simulator")
public interface SimulatorConfig {
    String siteId();

    @WithDefault("tcp://127.0.0.1:1883")
    String brokerUrl();

    /** Seed of the value sequence: the same seed and tick give the same reading values. */
    @WithDefault("126")
    long seed();

    @WithDefault("true")
    boolean enabled();

    /**
     * Assets as {@code assetId:metricName:unit:nominal:amplitude:breachValue}. Empty means the site's fixture assets
     * (scenario-controller Fixtures.java): site_ds9 asset_litterbox_transporter_ds9 (vibration), site_spacedock
     * asset_replicator_spacedock (temperature).
     */
    Optional<List<String>> assets();
}
