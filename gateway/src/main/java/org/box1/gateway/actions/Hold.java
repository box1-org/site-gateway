package org.box1.gateway.actions;

import java.time.Instant;

/** A hold placed at the site. */
public record Hold(String holdId, String sku, int quantity, String reasonCode, String status, Instant placedAt,
        Instant releasedAt) {
}
