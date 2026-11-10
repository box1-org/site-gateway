package org.box1.gateway;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;

/** Requests the gateway refuses before touching the database. */
@QuarkusTest
class SiteApiValidationTest {
    @Test
    void pickNeedsARequestIdAndAValidOrder() {
        given().contentType("application/json").body("{}").post("/api/v1/site/picks").then().statusCode(400)
                .body("code", is("REQUEST_ID"));
        given().contentType("application/json").body("{\"requestId\":\"0b0c7c5e-4f37-4d0e-8b1a-1d2a8c7e6f01\","
                + "\"orderId\":\"1; DROP TABLE outbox\"}").post("/api/v1/site/picks").then().statusCode(400)
                .body("code", is("ORDER_ID"));
    }

    @Test
    void countAndHoldBounds() {
        String id = "\"requestId\":\"0b0c7c5e-4f37-4d0e-8b1a-1d2a8c7e6f01\"";
        given().contentType("application/json").body("{" + id + ",\"sku\":\"SKU-42\",\"countedQuantity\":-1}")
                .post("/api/v1/site/counts").then().statusCode(400).body("code", is("QUANTITY"));
        given().contentType("application/json").body("{" + id + ",\"sku\":\" \",\"countedQuantity\":1}")
                .post("/api/v1/site/counts").then().statusCode(400).body("code", is("SKU"));
        given().contentType("application/json").body("{" + id + ",\"sku\":\"SKU-42\",\"quantity\":1,\"reasonCode\":\"LOST\"}")
                .post("/api/v1/site/holds").then().statusCode(400).body("code", is("REASON"));
        given().contentType("application/json").body("{" + id + "}").post("/api/v1/site/holds/hold_x/release").then()
                .statusCode(400).body("code", is("HOLD_ID"));
    }

    @Test
    void unknownFieldsAreRefused() {
        given().contentType("application/json").body("{\"requestId\":\"0b0c7c5e-4f37-4d0e-8b1a-1d2a8c7e6f01\","
                + "\"sku\":\"SKU-42\",\"countedQuantity\":1,\"site\":\"site_spacedock\"}").post("/api/v1/site/counts")
                .then().statusCode(400);
    }

    @Test
    void reportsTheSite() {
        given().get("/api/v1/site/info").then().statusCode(200).body("siteId", is("site_ds9"))
                .body("instance", is("site-west"));
    }
}
