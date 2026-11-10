package org.box1.simulator;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
class SimulatorTest {
    @Inject
    Simulator simulator;

    @Test
    void usesTheSitesFixtureAsset() {
        assertEquals("asset_replicator_spacedock", simulator.assets().get(0).assetId());
        assertEquals("asset_litterbox_transporter_ds9", SimulatedAsset.fixtures("site_ds9").get(0).assetId());
        assertTrue(SimulatedAsset.fixtures("site_utopia_planitia").isEmpty());
    }

    @Test
    void valuesAreDeterministicAndBelowWarning() {
        SimulatedAsset temperature = SimulatedAsset.fixtures("site_spacedock").get(0);
        SimulatedAsset vibration = SimulatedAsset.fixtures("site_ds9").get(0);
        for (long tick = 0; tick < 5000; tick++) {
            assertEquals(ReadingSequence.value(126, temperature, tick), ReadingSequence.value(126, temperature, tick));
            assertTrue(ReadingSequence.value(126, temperature, tick) < 75.0, "below the temperature clear level");
            assertTrue(ReadingSequence.value(126, vibration, tick) < 6.0, "below the vibration clear level");
        }
        assertTrue(temperature.breachValue() > 95.0 && vibration.breachValue() > 11.2, "breach is above critical");
    }

    @Test
    void messageMatchesTheGatewayFormat() throws Exception {
        String message = simulator.message(simulator.assets().get(0), Instant.parse("2026-11-10T12:00:00.123Z"), 61.5,
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        Map<?, ?> fields = new ObjectMapper().readValue(message, Map.class);
        assertEquals("site_spacedock", fields.get("siteId"));
        assertEquals("2026-11-10T12:00:00.123Z", fields.get("measuredAt"));
        assertEquals("temperature", fields.get("metricName"));
        assertEquals(7, fields.size());
    }

    @Test
    void assetSpecificationsAreValidated() {
        assertEquals(1.5, SimulatedAsset.parse("asset_a:pressure:bar:5:1.5:12").amplitude());
        assertThrows(IllegalArgumentException.class, () -> SimulatedAsset.parse("press:pressure:bar:5:1:12"));
        assertThrows(IllegalArgumentException.class, () -> SimulatedAsset.parse("asset_a:pressure"));
    }

    @Test
    void breachIsBounded() {
        given().contentType("application/json").body("{\"ticks\":0}").post("/api/v1/simulator/breach").then().statusCode(400);
        given().contentType("application/json").body("{\"ticks\":3}").post("/api/v1/simulator/breach").then().statusCode(200)
                .body("breachTicksRemaining", is(3));
    }
}
