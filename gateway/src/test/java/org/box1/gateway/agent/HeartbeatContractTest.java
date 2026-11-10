package org.box1.gateway.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.box1.gateway.contract.ContractSchemas;
import org.box1.gateway.outbox.BufferStats;
import org.junit.jupiter.api.Test;

/** The heartbeat body validates against SiteStatusReport in the contracts' asset-service OpenAPI (box1-docs#130). */
@QuarkusTest
class HeartbeatContractTest {
    private static final String DIGEST = "sha256:a11d6786b9a86378148ae55194acfd1cfb2d150ad298334fe00798d026abe3fe";

    @Inject
    HeartbeatAgent agent;

    @Test
    void reportWithKnownFactsValidates() throws Exception {
        Map<String, Object> report = agent.report(new BufferStats(12, 3, 40),
                new SiteFacts(DIGEST, null, DIGEST, "GREEN", DIGEST));
        ContractSchemas.assertValidAssetSchema("SiteStatusReport", report);
        assertEquals(15L, report.get("bufferDepth"));
        assertEquals(3L, report.get("siteEventBacklog"));
    }

    @Test
    void reportWithUnknownFactsValidates() throws Exception {
        ContractSchemas.assertValidAssetSchema("SiteStatusReport", agent.report(BufferStats.EMPTY, SiteFacts.UNKNOWN));
    }
}
