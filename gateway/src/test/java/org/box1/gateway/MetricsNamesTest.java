package org.box1.gateway;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

/** The buffer metrics the runbook, the in-guest collector and the cluster rely on (box1-docs#127, #132). */
@QuarkusTest
class MetricsNamesTest {
    @Test
    void bufferGaugesAreExportedWithTheSiteLabel() {
        given().get("/q/metrics").then().statusCode(200)
                .body(containsString("site_outbox_depth{class=\"reading\",site_id=\"site_ds9\"}"))
                .body(containsString("site_outbox_depth{class=\"site_event\",site_id=\"site_ds9\"}"))
                .body(containsString("site_outbox_oldest_age_seconds{site_id=\"site_ds9\"}"));
    }
}
