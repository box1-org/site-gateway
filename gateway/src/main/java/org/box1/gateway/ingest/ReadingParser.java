package org.box1.gateway.ingest;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.box1.gateway.outbox.Envelopes;

/**
 * Validates an MQTT reading message against the equipment.reading.v1 payload contract, plus the site rule that a
 * site only forwards readings for its own siteId. The message is the six payload fields and an optional traceparent.
 */
public final class ReadingParser {
    static final Pattern ASSET_ID = Pattern.compile("^asset_[A-Za-z0-9_-]{1,64}$");
    private static final Set<String> FIELDS = Set.of("assetId", "siteId", "measuredAt", "metricName", "value", "unit",
            "traceparent");
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {
    };
    private static final int MAX_BYTES = 4096;

    private final ObjectMapper mapper = new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    private final String siteId;

    public ReadingParser(String siteId) {
        this.siteId = siteId;
    }

    public InboundReading parse(byte[] body) throws RejectedReading {
        if (body == null || body.length == 0) {
            throw new RejectedReading("EMPTY");
        }
        if (body.length > MAX_BYTES) {
            throw new RejectedReading("TOO_LARGE");
        }
        Map<String, Object> fields;
        try {
            fields = mapper.readValue(body, MAP);
        } catch (IOException error) {
            throw new RejectedReading("NOT_JSON");
        }
        if (fields == null || !FIELDS.containsAll(fields.keySet())) {
            throw new RejectedReading("UNKNOWN_FIELD");
        }
        String assetId = text(fields, "assetId", 1, 70);
        if (!ASSET_ID.matcher(assetId).matches()) {
            throw new RejectedReading("ASSET_ID");
        }
        if (!siteId.equals(fields.get("siteId"))) {
            throw new RejectedReading("OTHER_SITE");
        }
        Instant measuredAt;
        try {
            measuredAt = Instant.parse(text(fields, "measuredAt", 1, 64));
        } catch (DateTimeParseException error) {
            throw new RejectedReading("MEASURED_AT");
        }
        String metricName = text(fields, "metricName", 1, 80);
        String unit = text(fields, "unit", 1, 20);
        if (!(fields.get("value") instanceof Number number) || !Double.isFinite(number.doubleValue())) {
            throw new RejectedReading("VALUE");
        }
        Object traceparent = fields.get("traceparent");
        if (traceparent != null && !(traceparent instanceof String text && Envelopes.isTraceparent(text))) {
            throw new RejectedReading("TRACEPARENT");
        }
        return new InboundReading(assetId, siteId, measuredAt, metricName, number.doubleValue(), unit,
                (String) traceparent);
    }

    private static String text(Map<String, Object> fields, String name, int min, int max) throws RejectedReading {
        if (fields.get(name) instanceof String text && text.length() >= min && text.length() <= max) {
            return text;
        }
        throw new RejectedReading(name.toUpperCase(java.util.Locale.ROOT));
    }
}
