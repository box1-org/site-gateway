package org.box1.gateway.actions;

import java.time.Instant;
import java.util.UUID;

/**
 * An operator action recorded at the site and queued as a site event.
 *
 * @param replayed true when the same requestId was already recorded and this is the earlier result
 */
public record ActionResult(String action, String localId, UUID eventId, String orderId, String sku, int quantity,
        Instant recordedAt, boolean replayed) {
}
