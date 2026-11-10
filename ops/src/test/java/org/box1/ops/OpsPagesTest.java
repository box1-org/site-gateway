package org.box1.ops;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.response.Response;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
class OpsPagesTest {
    @InjectMock
    @RestClient
    GatewayClient gateway;

    private static final GatewayClient.PickItem ITEM = new GatewayClient.PickItem("ord_2001", "res_2001", "SKU-42", 2,
            Instant.parse("2026-11-10T13:00:00Z"), "OPEN", Instant.parse("2026-11-10T12:00:00Z"), null, null, null);

    @BeforeEach
    void stubs() {
        when(gateway.info()).thenReturn(Map.of("siteId", "site_ds9", "instance", "site-west"));
        when(gateway.pickList(eq("OPEN"), any())).thenReturn(List.of(ITEM));
        when(gateway.pickList(eq("PICKED"), any())).thenReturn(List.of());
    }

    @Test
    void indexIsServedWithAStrictPolicyAndLocalHtmx() {
        given().get("/").then().statusCode(200).body(containsString("site_ds9"))
                .header("Content-Security-Policy", containsString("script-src 'self'"))
                .body(not(containsString("<script>")));
        given().get("/webjars/htmx.org/2.0.11/dist/htmx.min.js").then().statusCode(200);
        given().get("/ops.css").then().statusCode(200);
    }

    @Test
    void pickListRendersAFormWithCsrfAndRequestId() {
        given().get("/picks").then().statusCode(200).body(containsString("2 × SKU-42"))
                .body(containsString("name=\"csrf-token\"")).body(containsString("name=\"requestId\""));
    }

    @Test
    void aPostWithoutTheCsrfTokenIsRefused() {
        given().contentType("application/x-www-form-urlencoded").formParam("requestId", UUID.randomUUID().toString())
                .post("/picks/ord_2001").then().statusCode(400);
        verify(gateway, never()).pick(any());
    }

    @Test
    void aConfirmedPickAndAFullBufferAreReported() {
        when(gateway.pick(any())).thenReturn(new GatewayClient.ActionResult("PICK", "pick_x", UUID.randomUUID(),
                "ord_2001", "SKU-42", 2, Instant.now(), false));
        post("/picks/ord_2001", Map.of()).then().statusCode(200).body(containsString("Picked 2 × SKU-42 for ord_2001"));
        when(gateway.count(any())).thenThrow(new WebApplicationException(503));
        post("/counts", Map.of("sku", "SKU-42", "countedQuantity", "4")).then().statusCode(200)
                .body(containsString("buffer is full"));
    }

    @Test
    void anUnreachableGatewayRecordsNothingAndSaysSo() {
        when(gateway.hold(any())).thenThrow(new ProcessingException("connection refused"));
        when(gateway.holds(any())).thenThrow(new ProcessingException("connection refused"));
        post("/holds", Map.of("sku", "SKU-7", "quantity", "1", "reasonCode", "DAMAGED")).then().statusCode(200)
                .body(containsString("not responding"));
    }

    @Test
    void invalidFormValuesNeverReachTheGateway() {
        post("/counts", Map.of("sku", "SKU-42", "countedQuantity", "lots")).then().statusCode(200)
                .body(containsString("whole number"));
        verify(gateway, never()).count(any());
    }

    private static Response post(String path, Map<String, String> fields) {
        Response page = given().get("/picks");
        String token = page.getCookie("csrf-token");
        var request = given().contentType("application/x-www-form-urlencoded").cookie("csrf-token", token)
                .formParam("csrf-token", token).formParam("requestId", UUID.randomUUID().toString());
        fields.forEach(request::formParam);
        return request.post(path);
    }
}
