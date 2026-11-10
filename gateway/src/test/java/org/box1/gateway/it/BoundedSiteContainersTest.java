package org.box1.gateway.it;

import static io.restassured.RestAssured.given;
import static org.box1.gateway.it.Fixtures.await;
import static org.box1.gateway.it.Fixtures.count;
import static org.box1.gateway.it.Fixtures.quietly;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.box1.gateway.outbox.OutboxRow;
import org.box1.gateway.outbox.OutboxStore;
import org.box1.gateway.publish.KafkaSender;
import org.box1.gateway.publish.OutboxPublisher;
import org.box1.gateway.support.Db;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * The buffer bound and the crash case, with the scheduled publisher off (box1-docs#127): readings over the bound are
 * dropped oldest first and recorded; site events beyond theirs are refused; rows sent but not committed are re-sent
 * with the same eventId.
 */
@QuarkusTest
@TestProfile(BoundedProfile.class)
@Tag("containers")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BoundedSiteContainersTest {
    @Inject
    Db db;
    @Inject
    OutboxStore store;
    @Inject
    KafkaSender sender;
    @Inject
    OutboxPublisher publisher;

    @Test
    @Order(1)
    void readingsOverTheBoundDropTheOldestAndAreRecorded() throws Exception {
        Instant base = Instant.now().truncatedTo(ChronoUnit.SECONDS).minusSeconds(60);
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            messages.add(Fixtures.reading("asset_bound_ds9", "site_ds9", base.plusSeconds(i), i));
        }
        Fixtures.mqttPublish(messages);
        await("8 readings handled", Duration.ofSeconds(30), quietly(() -> count(db.dataSource(),
                "SELECT coalesce(sum(rows_dropped), 0) FROM outbox_drop WHERE reason = 'SIZE'") == 3));
        assertEquals(5, count(db.dataSource(), "SELECT count(*) FROM outbox WHERE published_at IS NULL AND event_class = 'READING'"));
        assertEquals(0, count(db.dataSource(), "SELECT count(*) FROM outbox WHERE envelope LIKE ?",
                "%\"measuredAt\":\"" + base.plusSeconds(2) + "\"%"), "the oldest were dropped");
        assertEquals(1, count(db.dataSource(), "SELECT count(*) FROM outbox WHERE envelope LIKE ?",
                "%\"measuredAt\":\"" + base.plusSeconds(7) + "\"%"), "the newest was kept");
    }

    @Test
    @Order(2)
    void siteEventsBeyondTheBoundAreRefusedNotDropped() {
        for (int i = 0; i < 2; i++) {
            given().contentType("application/json").body("{\"requestId\":\"" + UUID.randomUUID()
                    + "\",\"sku\":\"SKU-42\",\"countedQuantity\":" + i + "}").post("/api/v1/site/counts").then().statusCode(201);
        }
        given().contentType("application/json").body("{\"requestId\":\"" + UUID.randomUUID()
                + "\",\"sku\":\"SKU-42\",\"countedQuantity\":9}").post("/api/v1/site/counts").then().statusCode(503)
                .body("code", is("BUFFER_FULL"));
        given().contentType("application/json").body("{\"requestId\":\"" + UUID.randomUUID()
                + "\",\"sku\":\"SKU-7\",\"quantity\":1,\"reasonCode\":\"DAMAGED\"}").post("/api/v1/site/holds").then()
                .statusCode(503);
    }

    @Test
    @Order(3)
    void aCrashAfterTheAcknowledgementResendsTheSameEventIds() throws Exception {
        // Send every pending row and wait for the acknowledgements, then roll back instead of committing: a crash
        // between Kafka's acknowledgement and the outbox commit.
        List<String> sent = new ArrayList<>();
        try (Connection connection = db.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            for (OutboxRow row : store.lockPending(connection, 100)) {
                sender.send(row).get();
                sent.add(row.eventId().toString());
            }
            connection.rollback();
        }
        assertEquals(7, sent.size(), "5 readings and 2 site events");
        assertEquals(7, count(db.dataSource(), "SELECT count(*) FROM outbox WHERE published_at IS NULL"),
                "nothing committed was lost");
        while (publisher.drainOnce() > 0) {
            // drain
        }
        assertEquals(0, count(db.dataSource(), "SELECT count(*) FROM outbox WHERE published_at IS NULL"));
        List<String> onKafka = new ArrayList<>();
        for (String topic : List.of("box1.telemetry.events.v1", "box1.site.events.v1")) {
            for (ConsumerRecord<String, String> record : Fixtures.readAll(topic, Duration.ofSeconds(4))) {
                sent.stream().filter(id -> record.value().contains(id)).forEach(onKafka::add);
            }
        }
        assertEquals(14, onKafka.size(), "each row twice");
        assertEquals(java.util.Set.copyOf(sent), java.util.Set.copyOf(onKafka), "duplicates keep their eventId");
    }
}
