package org.box1.gateway.ingest;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Test access to the package-private envelope builder. */
public final class ReadingIngestAccess {
    private ReadingIngestAccess() {
    }

    public static Map<String, Object> envelope(UUID eventId, InboundReading reading, Instant occurredAt,
            String traceparent) {
        return ReadingIngest.envelope(eventId, reading, occurredAt, traceparent);
    }
}
