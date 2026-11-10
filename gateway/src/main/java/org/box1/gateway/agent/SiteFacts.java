package org.box1.gateway.agent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * OS facts the site agent reports, read from the JSON file that site-gateway-os's root-owned box1-site-facts service
 * writes from {@code bootc status --format json}, greenboot and the image (so the gateway needs no privilege).
 * Unknown or malformed values become null / UNKNOWN; they are never passed through unchecked.
 */
public record SiteFacts(String bootedImageDigest, String stagedImageDigest, String rollbackImageDigest,
        String greenbootStatus, String bundleDigest) {
    public static final SiteFacts UNKNOWN = new SiteFacts(null, null, null, "UNKNOWN", null);
    private static final Pattern DIGEST = Pattern.compile("^sha256:[a-f0-9]{64}$");
    private static final Set<String> GREENBOOT = Set.of("GREEN", "RED", "UNKNOWN");
    private static final long MAX_BYTES = 64 * 1024;

    public static SiteFacts read(Path file, ObjectMapper mapper) {
        try {
            if (!Files.isRegularFile(file) || Files.size(file) > MAX_BYTES) {
                return UNKNOWN;
            }
            JsonNode node = mapper.readTree(file.toFile());
            String greenboot = node.path("greenbootStatus").asText("UNKNOWN");
            return new SiteFacts(digest(node, "bootedImageDigest"), digest(node, "stagedImageDigest"),
                    digest(node, "rollbackImageDigest"), GREENBOOT.contains(greenboot) ? greenboot : "UNKNOWN",
                    digest(node, "bundleDigest"));
        } catch (IOException error) {
            return UNKNOWN;
        }
    }

    private static String digest(JsonNode node, String field) {
        String value = node.path(field).asText(null);
        return value != null && DIGEST.matcher(value).matches() ? value : null;
    }
}
