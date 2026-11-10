package org.box1.gateway.outbox;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * The shared v1 event envelope (box1-ops-contracts events/v1/envelope.schema.json) and the reading eventId rule
 * (TELEMETRY.md): the same reading always gets the same eventId, so a re-sent reading is a harmless duplicate.
 */
public final class Envelopes {
    public static final String READING_EVENT = "equipment.reading.v1";
    public static final String PICK_CONFIRMED = "site.pick-confirmed.v1";
    public static final String STOCK_ADJUSTED = "site.stock-adjusted.v1";
    public static final String STOCK_HELD = "site.stock-held.v1";
    /** causationId of every reading ingested at a site (SITE.md). */
    public static final String SITE_INGEST = "ingest:site";

    private static final Pattern TRACEPARENT = Pattern.compile("^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$");

    private Envelopes() {
    }

    /**
     * Name-based (RFC 4122 version 3) UUID over the reading's natural key, exactly as asset-service's
     * IngestionPipeline.eventId and TELEMETRY.md define it. Not for security.
     */
    public static UUID readingEventId(String assetId, String metricName, Instant measuredAt) {
        String key = "box1/" + READING_EVENT + "/" + assetId + "/" + metricName + "/" + measuredAt;
        return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8));
    }

    public static Map<String, Object> envelope(UUID eventId, String eventType, String aggregateId, Instant occurredAt,
            String correlationId, String causationId, String traceparent, Map<String, Object> payload) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventId", eventId.toString());
        event.put("eventType", eventType);
        event.put("schemaVersion", 1);
        event.put("aggregateId", aggregateId);
        event.put("occurredAt", occurredAt.toString());
        event.put("correlationId", correlationId);
        event.put("causationId", causationId);
        if (isTraceparent(traceparent)) {
            event.put("traceparent", traceparent);
        }
        event.put("payload", payload);
        return event;
    }

    public static boolean isTraceparent(String value) {
        return value != null && TRACEPARENT.matcher(value).matches();
    }
}
