package org.box1.gateway.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Files;
import java.nio.file.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SiteFactsTest {
    private static final String DIGEST = "sha256:a11d6786b9a86378148ae55194acfd1cfb2d150ad298334fe00798d026abe3fe";
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void readsValidFacts(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("facts.json");
        Files.writeString(file, "{\"bootedImageDigest\":\"" + DIGEST + "\",\"stagedImageDigest\":null,"
                + "\"rollbackImageDigest\":\"" + DIGEST + "\",\"greenbootStatus\":\"RED\",\"bundleDigest\":\"" + DIGEST + "\"}");
        SiteFacts facts = SiteFacts.read(file, mapper);
        assertEquals(DIGEST, facts.bootedImageDigest());
        assertNull(facts.stagedImageDigest());
        assertEquals("RED", facts.greenbootStatus());
    }

    @Test
    void unknownWhenMissingOrMalformed(@TempDir Path dir) throws Exception {
        assertEquals(SiteFacts.UNKNOWN, SiteFacts.read(dir.resolve("absent.json"), mapper));
        Path bad = dir.resolve("bad.json");
        Files.writeString(bad, "{\"bootedImageDigest\":\"sha256:XYZ\",\"greenbootStatus\":\"PURPLE\"}");
        SiteFacts facts = SiteFacts.read(bad, mapper);
        assertNull(facts.bootedImageDigest());
        assertEquals("UNKNOWN", facts.greenbootStatus());
        Files.writeString(bad, "not json");
        assertEquals(SiteFacts.UNKNOWN, SiteFacts.read(bad, mapper));
    }
}
