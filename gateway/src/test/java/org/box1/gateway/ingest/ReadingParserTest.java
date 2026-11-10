package org.box1.gateway.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.junit.jupiter.api.Test;

class ReadingParserTest {
    private final ReadingParser parser = new ReadingParser("site_ds9");

    private static byte[] json(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private String reject(String body) {
        return assertThrows(RejectedReading.class, () -> parser.parse(json(body))).reason();
    }

    @Test
    void acceptsAValidReading() throws Exception {
        InboundReading reading = parser.parse(json("{\"assetId\":\"asset_litterbox_transporter_ds9\",\"siteId\":\"site_ds9\","
                + "\"measuredAt\":\"2026-11-10T12:00:05.100Z\",\"metricName\":\"vibration\",\"value\":3.2,\"unit\":\"mm/s\","
                + "\"traceparent\":\"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\"}"));
        assertEquals(Instant.parse("2026-11-10T12:00:05.100Z"), reading.measuredAt());
        assertEquals(3.2, reading.value());
        assertEquals("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", reading.traceparent());
    }

    @Test
    void traceparentIsOptional() throws Exception {
        assertNull(parser.parse(json("{\"assetId\":\"asset_a\",\"siteId\":\"site_ds9\",\"measuredAt\":\"2026-11-10T12:00:05Z\","
                + "\"metricName\":\"vibration\",\"value\":1,\"unit\":\"mm/s\"}")).traceparent());
    }

    @Test
    void aSiteForwardsOnlyItsOwnReadings() {
        assertEquals("OTHER_SITE", reject("{\"assetId\":\"asset_a\",\"siteId\":\"site_spacedock\","
                + "\"measuredAt\":\"2026-11-10T12:00:05Z\",\"metricName\":\"t\",\"value\":1,\"unit\":\"C\"}"));
    }

    @Test
    void rejectsMalformedMessages() {
        String valid = "\"siteId\":\"site_ds9\",\"measuredAt\":\"2026-11-10T12:00:05Z\",\"metricName\":\"t\",\"unit\":\"C\"";
        assertEquals("NOT_JSON", reject("{"));
        assertEquals("UNKNOWN_FIELD", reject("{\"assetId\":\"asset_a\"," + valid + ",\"value\":1,\"extra\":1}"));
        assertEquals("ASSET_ID", reject("{\"assetId\":\"press_7\"," + valid + ",\"value\":1}"));
        assertEquals("VALUE", reject("{\"assetId\":\"asset_a\"," + valid + ",\"value\":\"1\"}"));
        assertEquals("MEASURED_AT", reject("{\"assetId\":\"asset_a\",\"siteId\":\"site_ds9\",\"measuredAt\":\"yesterday\","
                + "\"metricName\":\"t\",\"unit\":\"C\",\"value\":1}"));
        assertEquals("TRACEPARENT", reject("{\"assetId\":\"asset_a\"," + valid + ",\"value\":1,\"traceparent\":\"x\"}"));
        assertEquals("NOT_JSON", reject("{\"assetId\":\"asset_a\",\"assetId\":\"asset_b\"," + valid + ",\"value\":1}"));
        assertEquals("EMPTY", assertThrows(RejectedReading.class, () -> parser.parse(new byte[0])).reason());
        assertEquals("TOO_LARGE", assertThrows(RejectedReading.class, () -> parser.parse(new byte[5000])).reason());
    }
}
