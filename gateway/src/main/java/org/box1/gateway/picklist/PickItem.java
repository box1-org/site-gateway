package org.box1.gateway.picklist;

import java.time.Instant;

/** One reservation on the site's pick list. */
public record PickItem(String orderId, String reservationId, String sku, int quantity, Instant expiresAt,
        String status, Instant receivedAt, String pickId, Instant pickedAt, Instant closedAt) {
}
