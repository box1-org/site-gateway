package org.box1.gateway.outbox;

import java.util.UUID;

/** An unpublished outbox row, as the publisher sends it. */
public record OutboxRow(long seq, UUID eventId, EventClass eventClass, String topic, String key, String envelope) {
}
