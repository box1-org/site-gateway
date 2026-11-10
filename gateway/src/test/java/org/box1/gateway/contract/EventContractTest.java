package org.box1.gateway.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.box1.gateway.ingest.InboundReading;
import org.box1.gateway.ingest.ReadingIngestAccess;
import org.box1.gateway.outbox.Envelopes;
import org.box1.gateway.outbox.SiteEvents;
import org.junit.jupiter.api.Test;

/** Every event the gateway produces validates against box1-ops-contracts at contracts.ref (box1-docs#127). */
class EventContractTest {
    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Test
    void readingEventIdMatchesTheContractTestVector() {
        // box1-ops-contracts fixtures/events.json, the site_spacedock reading (TELEMETRY.md derivation).
        assertEquals(UUID.fromString("c127b8e5-a509-34d4-a82c-fe3e11618a8e"), Envelopes.readingEventId(
                "asset_replicator_spacedock", "temperature", Instant.parse("2026-11-10T12:00:00Z")));
    }

    @Test
    void aResentReadingKeepsItsEventIdAndAnotherInstantDoesNot() {
        Instant at = Instant.parse("2026-11-10T12:00:00.250Z");
        assertEquals(Envelopes.readingEventId("asset_a", "vibration", at),
                Envelopes.readingEventId("asset_a", "vibration", Instant.parse("2026-11-10T12:00:00.250Z")));
        assertFalse(Envelopes.readingEventId("asset_a", "vibration", at)
                .equals(Envelopes.readingEventId("asset_a", "vibration", at.plusMillis(1))));
    }

    @Test
    void readingEnvelopeValidates() throws Exception {
        InboundReading reading = new InboundReading("asset_litterbox_transporter_ds9", "site_ds9",
                Instant.parse("2026-11-10T12:00:05.100Z"), "vibration", 3.2, "mm/s", TRACEPARENT);
        UUID eventId = Envelopes.readingEventId(reading.assetId(), reading.metricName(), reading.measuredAt());
        Map<String, Object> event = ReadingIngestAccess.envelope(eventId, reading, Instant.parse("2026-11-10T12:00:05.2Z"),
                TRACEPARENT);
        ContractSchemas.assertValidEvent(event);
        assertEquals("ingest:site", event.get("causationId"));
        assertEquals("asset_litterbox_transporter_ds9", event.get("aggregateId"));
    }

    @Test
    void siteEventEnvelopesValidate() throws Exception {
        Instant at = Instant.parse("2026-11-10T12:05:00Z");
        String hex = "0123456789abcdef0123456789abcdef";
        ContractSchemas.assertValidEvent(SiteEvents.pickConfirmed(UUID.randomUUID(), "site_ds9", "pick_" + hex,
                "ord_2001", "res_2001", "SKU-42", 2, at, UUID.randomUUID(), TRACEPARENT));
        ContractSchemas.assertValidEvent(SiteEvents.stockAdjusted(UUID.randomUUID(), "site_ds9", "cnt_" + hex,
                "SKU-42", 0, at, null));
        ContractSchemas.assertValidEvent(SiteEvents.stockHeld(UUID.randomUUID(), "site_spacedock", "hold_" + hex,
                "SKU-7", 1, "RELEASED", "QUALITY_CHECK", at, TRACEPARENT));
    }

    @Test
    void theValidatorRejectsAnInvalidSiteEvent() {
        Map<String, Object> bad = SiteEvents.stockAdjusted(UUID.randomUUID(), "site_ds9", "cnt_x", "SKU-42", -1,
                Instant.parse("2026-11-10T12:05:00Z"), null);
        assertThrows(AssertionError.class, () -> ContractSchemas.assertValidEvent(bad));
    }
}
