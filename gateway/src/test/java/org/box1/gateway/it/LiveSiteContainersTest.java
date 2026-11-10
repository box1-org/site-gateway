package org.box1.gateway.it;

import static io.restassured.RestAssured.given;
import static org.box1.gateway.it.Fixtures.await;
import static org.box1.gateway.it.Fixtures.count;
import static org.box1.gateway.it.Fixtures.quietly;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.box1.gateway.contract.ContractSchemas;
import org.box1.gateway.outbox.Envelopes;
import org.box1.gateway.support.Db;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * End to end against real PostgreSQL, Kafka and an Artemis MQTT broker (box1-docs#127, #129): MQTT → outbox → Kafka,
 * duplicates, a Kafka outage and a database outage without loss, and the pick list with pick confirmation.
 */
@QuarkusTest
@TestProfile(LiveProfile.class)
@Tag("containers")
class LiveSiteContainersTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {
    };

    @Inject
    Db db;
    @Inject
    org.apache.camel.CamelContext camel;

    private static Map<String, Object> parse(String value) throws Exception {
        return JSON.readValue(value, MAP);
    }

    private long pending(String eventClass) throws java.sql.SQLException {
        return count(db.dataSource(), "SELECT count(*) FROM outbox WHERE published_at IS NULL AND event_class = ?",
                eventClass);
    }

    @Test
    void readyWithDatabaseAndBroker() throws Exception {
        await("readiness", Duration.ofSeconds(60), () -> given().get("/q/health/ready").statusCode() == 200);
        given().get("/q/health/ready").then().body("status", is("UP"));
    }

    @Test
    void aReadingReachesKafkaOnceWithAValidEnvelope() throws Exception {
        Instant at = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        String message = Fixtures.reading("asset_litterbox_transporter_ds9", "site_ds9", at, 3.4);
        Fixtures.mqttPublish(List.of(message, message, Fixtures.reading("asset_x", "site_spacedock", at, 1)));
        UUID eventId = Envelopes.readingEventId("asset_litterbox_transporter_ds9", "vibration", at);
        await("publication", Duration.ofSeconds(60), quietly(() -> count(db.dataSource(),
                "SELECT count(*) FROM outbox WHERE event_id = ? AND published_at IS NOT NULL", eventId) == 1));
        List<ConsumerRecord<String, String>> records = Fixtures.readAll("box1.telemetry.events.v1", Duration.ofSeconds(5))
                .stream().filter(r -> r.value().contains(eventId.toString())).toList();
        assertEquals(1, records.size(), "the duplicate MQTT message is one record");
        ConsumerRecord<String, String> record = records.get(0);
        assertEquals("asset_litterbox_transporter_ds9", record.key());
        assertEquals("site_ds9", new String(record.headers().lastHeader("box1-site-id").value(), StandardCharsets.UTF_8));
        Map<String, Object> event = parse(record.value());
        ContractSchemas.assertValidEvent(event);
        assertEquals(eventId.toString(), event.get("eventId"));
        assertEquals("ingest:site", event.get("causationId"));
        assertTrue(((String) event.get("traceparent")).startsWith("00-4bf92f3577b34da6a3ce929d0e0e4736-"),
                "the trace the simulator started continues");
        assertEquals(0, count(db.dataSource(), "SELECT count(*) FROM outbox WHERE envelope LIKE '%site_spacedock%'"),
                "another site's reading is not forwarded");
    }

    @Test
    void aKafkaOutageBuffersAndTheBacklogDrainsWithoutLossOrDuplicates() throws Exception {
        Instant base = Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(600);
        List<String> messages = new ArrayList<>();
        Set<String> expected = new java.util.HashSet<>();
        for (int i = 0; i < 20; i++) {
            Instant at = base.plusMillis(i * 100L);
            messages.add(Fixtures.reading("asset_outage_ds9", "site_ds9", at, i));
            expected.add(Envelopes.readingEventId("asset_outage_ds9", "vibration", at).toString());
        }
        SiteContainers.pause(SiteContainers.kafka);
        try {
            Fixtures.mqttPublish(messages);
            await("20 buffered readings", Duration.ofSeconds(30), quietly(() -> count(db.dataSource(),
                    "SELECT count(*) FROM outbox WHERE published_at IS NULL AND record_key = 'asset_outage_ds9'") == 20));
            await("buffer depth visible", Duration.ofSeconds(10),
                    () -> given().get("/api/v1/site/buffer").then().extract().<Integer>path("readings") >= 20);
        } finally {
            SiteContainers.unpause(SiteContainers.kafka);
        }
        await("drain", Duration.ofMinutes(3), quietly(() -> pending("READING") == 0));
        List<String> ids = new ArrayList<>();
        for (ConsumerRecord<String, String> record : Fixtures.readAll("box1.telemetry.events.v1", Duration.ofSeconds(5))) {
            if ("asset_outage_ds9".equals(record.key())) {
                ids.add((String) parse(record.value()).get("eventId"));
            }
        }
        assertEquals(expected, Set.copyOf(ids), "no loss");
        assertEquals(20, ids.size(), "no duplicates");
    }

    @Test
    void readingsQueuedWhileTheGatewayIsDisconnectedArriveWhenItReconnects() throws Exception {
        await("route started", Duration.ofSeconds(60), () -> given().get("/q/health/ready").statusCode() == 200);
        camel.getRouteController().stopRoute(org.box1.gateway.ingest.ReadingRoute.ROUTE_ID);
        Instant base = Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(300);
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            messages.add(Fixtures.reading("asset_backlog_ds9", "site_ds9", base.plusMillis(i * 10L), i));
        }
        Fixtures.mqttPublish(messages);
        camel.getRouteController().startRoute(org.box1.gateway.ingest.ReadingRoute.ROUTE_ID);
        await("the broker's backlog is recorded", Duration.ofSeconds(30), quietly(() -> count(db.dataSource(),
                "SELECT count(*) FROM outbox WHERE record_key = 'asset_backlog_ds9'") == 10));
    }

    @Test
    void aDatabaseOutageHoldsTheReadingUntilItIsStored() throws Exception {
        Instant at = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        UUID eventId = Envelopes.readingEventId("asset_dbdown_ds9", "vibration", at);
        SiteContainers.pause(SiteContainers.postgres);
        try {
            Fixtures.mqttPublish(List.of(Fixtures.reading("asset_dbdown_ds9", "site_ds9", at, 2)));
            Thread.sleep(3000);
        } finally {
            SiteContainers.unpause(SiteContainers.postgres);
        }
        await("the held reading is stored", Duration.ofSeconds(90), quietly(() -> count(db.dataSource(),
                "SELECT count(*) FROM outbox WHERE event_id = ?", eventId) == 1));
    }

    @Test
    void pickListKeepsOnlyThisSiteAndAPickBecomesOneSiteEvent() throws Exception {
        String reservedEventId = UUID.randomUUID().toString();
        // Schema-valid, but PostgreSQL rejects a NUL in text: it is skipped and must not block the records after it.
        Fixtures.kafkaSend("box1.inventory.events.v1", "ord_nul", reserved(UUID.randomUUID().toString(), "ord_nul",
                "res_nul", "site_ds9").replace("\"sku\":\"SKU-42\"", "\"sku\":\"SKU\\u0000\""));
        Fixtures.kafkaSend("box1.inventory.events.v1", "ord_it1", reserved(reservedEventId, "ord_it1", "res_it1", "site_ds9"));
        Fixtures.kafkaSend("box1.inventory.events.v1", "ord_it2", reserved(UUID.randomUUID().toString(), "ord_it2",
                "res_it2", "site_spacedock"));
        Fixtures.kafkaSend("box1.fulfillment.events.v1", "ord_it3", shipped("ord_it3"));
        Fixtures.kafkaSend("box1.inventory.events.v1", "ord_it3", reserved(UUID.randomUUID().toString(), "ord_it3",
                "res_it3", "site_ds9"));
        await("pick list", Duration.ofSeconds(60), quietly(() -> count(db.dataSource(),
                "SELECT count(*) FROM pick_item WHERE order_id IN ('ord_it1', 'ord_it3')") == 2));
        assertEquals(0, count(db.dataSource(), "SELECT count(*) FROM pick_item WHERE order_id = 'ord_nul'"),
                "a record the database rejects is skipped");
        assertEquals(0, count(db.dataSource(), "SELECT count(*) FROM pick_item WHERE order_id = 'ord_it2'"),
                "another site's reservation is not on this pick list");
        given().get("/api/v1/site/pick-list?status=SHIPPED").then().statusCode(200);
        assertEquals(1, count(db.dataSource(),
                "SELECT count(*) FROM pick_item WHERE order_id = 'ord_it3' AND status = 'SHIPPED'"),
                "a shipment that arrived first keeps the reservation closed");

        String requestId = UUID.randomUUID().toString();
        String body = "{\"requestId\":\"" + requestId + "\",\"orderId\":\"ord_it1\"}";
        String pickId = given().contentType("application/json").body(body).post("/api/v1/site/picks").then()
                .statusCode(201).extract().path("localId");
        given().contentType("application/json").body(body).post("/api/v1/site/picks").then().statusCode(200)
                .body("localId", is(pickId)).body("replayed", is(true));
        given().contentType("application/json").body("{\"requestId\":\"" + UUID.randomUUID()
                + "\",\"orderId\":\"ord_it1\"}").post("/api/v1/site/picks").then().statusCode(409).body("code", is("NOT_OPEN"));

        await("site event published", Duration.ofSeconds(60), quietly(() -> pending("SITE_EVENT") == 0));
        List<ConsumerRecord<String, String>> events = Fixtures.readAll("box1.site.events.v1", Duration.ofSeconds(5))
                .stream().filter(r -> r.value().contains(pickId)).collect(Collectors.toList());
        assertEquals(1, events.size());
        assertEquals("site_ds9", events.get(0).key());
        Map<String, Object> event = parse(events.get(0).value());
        ContractSchemas.assertValidEvent(event);
        assertEquals(reservedEventId, event.get("causationId"));
        assertEquals("ord_it1", event.get("correlationId"));

        Fixtures.kafkaSend("box1.fulfillment.events.v1", "ord_it1", shipped("ord_it1"));
        await("shipped", Duration.ofSeconds(60), quietly(() -> count(db.dataSource(),
                "SELECT count(*) FROM pick_item WHERE order_id = 'ord_it1' AND status = 'SHIPPED'") == 1));
    }

    private static String reserved(String eventId, String orderId, String reservationId, String siteId) {
        return "{\"eventId\":\"" + eventId + "\",\"eventType\":\"inventory.reserved.v1\",\"schemaVersion\":1,"
                + "\"aggregateId\":\"" + orderId + "\",\"occurredAt\":\"" + Instant.now() + "\",\"correlationId\":\"c\","
                + "\"causationId\":\"x\",\"payload\":{\"orderId\":\"" + orderId + "\",\"reservationId\":\"" + reservationId
                + "\",\"sku\":\"SKU-42\",\"quantity\":2,\"siteId\":\"" + siteId + "\",\"expiresAt\":\""
                + Instant.now().plusSeconds(3600) + "\"}}";
    }

    private static String shipped(String orderId) {
        return "{\"eventId\":\"" + UUID.randomUUID() + "\",\"eventType\":\"shipment.confirmed.v1\",\"schemaVersion\":1,"
                + "\"aggregateId\":\"" + orderId + "\",\"occurredAt\":\"" + Instant.now() + "\",\"correlationId\":\"c\","
                + "\"causationId\":\"x\",\"payload\":{\"orderId\":\"" + orderId + "\",\"fulfillmentRequestId\":\"ful_1\","
                + "\"shipmentId\":\"shp_1\",\"shippedAt\":\"" + Instant.now() + "\"}}";
    }
}
