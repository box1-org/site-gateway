package org.box1.gateway.outbox;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/** Envelopes of the site.*.v1 events (box1-ops-contracts SITE.md): aggregateId and key are the siteId. */
public final class SiteEvents {
    private SiteEvents() {
    }

    public static Map<String, Object> pickConfirmed(UUID eventId, String siteId, String pickId, String orderId,
            String reservationId, String sku, int quantity, Instant pickedAt, UUID reservedEventId, String traceparent) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("siteId", siteId);
        payload.put("pickId", pickId);
        payload.put("orderId", orderId);
        payload.put("reservationId", reservationId);
        payload.put("sku", sku);
        payload.put("quantity", quantity);
        payload.put("pickedAt", pickedAt.toString());
        return Envelopes.envelope(eventId, Envelopes.PICK_CONFIRMED, siteId, pickedAt, orderId,
                reservedEventId.toString(), traceparent, payload);
    }

    public static Map<String, Object> stockAdjusted(UUID eventId, String siteId, String countId, String sku,
            int countedQuantity, Instant countedAt, String traceparent) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("siteId", siteId);
        payload.put("countId", countId);
        payload.put("sku", sku);
        payload.put("countedQuantity", countedQuantity);
        payload.put("countedAt", countedAt.toString());
        payload.put("reason", "CYCLE_COUNT");
        return Envelopes.envelope(eventId, Envelopes.STOCK_ADJUSTED, siteId, countedAt, countId, "site-ops:count",
                traceparent, payload);
    }

    public static Map<String, Object> stockHeld(UUID eventId, String siteId, String holdId, String sku, int quantity,
            String action, String reasonCode, Instant changedAt, String traceparent) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("siteId", siteId);
        payload.put("holdId", holdId);
        payload.put("sku", sku);
        payload.put("quantity", quantity);
        payload.put("action", action);
        payload.put("reasonCode", reasonCode);
        payload.put("changedAt", changedAt.toString());
        return Envelopes.envelope(eventId, Envelopes.STOCK_HELD, siteId, changedAt, holdId, "site-ops:hold",
                traceparent, payload);
    }
}
