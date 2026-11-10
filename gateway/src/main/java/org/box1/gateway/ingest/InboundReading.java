package org.box1.gateway.ingest;

import java.time.Instant;

/** A reading from the site's equipment, as received over MQTT and validated. */
public record InboundReading(String assetId, String siteId, Instant measuredAt, String metricName, double value,
        String unit, String traceparent) {
}
