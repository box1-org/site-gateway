package org.box1.gateway.picklist;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.box1.gateway.config.SiteConfig;
import org.box1.gateway.support.Db;
import org.jboss.logging.Logger;

/**
 * Keeps the site's pick list from Kafka (box1-docs#129, SITE.md): inventory.reserved.v1 for this site adds an item;
 * inventory.reservation-expired.v1 and shipment.confirmed.v1 close it. Group site-gateway-<siteId>. Offsets are
 * committed only after the batch's effects are committed locally; a failure seeks back and the batch is replayed,
 * which is harmless because every effect is idempotent. While Kafka is unreachable the pick list stays usable.
 */
@ApplicationScoped
public class PickListConsumer {
    private static final Logger LOG = Logger.getLogger(PickListConsumer.class);
    private static final Pattern ORDER_ID = Pattern.compile("^ord_[A-Za-z0-9_-]+$");
    private static final Pattern RESERVATION_ID = Pattern.compile("^res_[A-Za-z0-9_-]+$");
    private static final Duration RETENTION = Duration.ofDays(15);

    private final SiteConfig config;
    private final Db db;
    private final PickListStore store;
    private final Clock clock;
    private final MeterRegistry registry;
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean running;
    private volatile KafkaConsumer<String, byte[]> consumer;
    private Thread thread;

    public PickListConsumer(SiteConfig config, Db db, PickListStore store, Clock clock, MeterRegistry registry) {
        this.config = config;
        this.db = db;
        this.store = store;
        this.clock = clock;
        this.registry = registry;
    }

    void start(@Observes StartupEvent event) {
        if (!config.kafka().pickListEnabled()) {
            return;
        }
        running = true;
        thread = new Thread(this::run, "site-pick-list");
        thread.setDaemon(true);
        thread.start();
    }

    void stop(@Observes ShutdownEvent event) throws InterruptedException {
        running = false;
        KafkaConsumer<String, byte[]> current = consumer;
        if (current != null) {
            current.wakeup();
        }
        if (thread != null) {
            thread.join(Duration.ofSeconds(10).toMillis());
        }
    }

    public String groupId() {
        return "site-gateway-" + config.id();
    }

    /** Applies one record. Returns the outcome used as a metric tag. Never throws for a bad record. */
    String apply(Connection connection, byte[] value) throws SQLException {
        JsonNode event;
        try {
            event = value == null ? null : mapper.readTree(value);
        } catch (IOException error) {
            return "poison";
        }
        if (event == null || !event.path("payload").isObject()) {
            return "poison";
        }
        JsonNode payload = event.path("payload");
        String type = event.path("eventType").asText("");
        Instant now = clock.instant();
        try {
            switch (type) {
                case "inventory.reserved.v1" -> {
                    if (!config.id().equals(payload.path("siteId").asText())) {
                        return "other_site";
                    }
                    String orderId = id(payload, "orderId", ORDER_ID);
                    String reservationId = id(payload, "reservationId", RESERVATION_ID);
                    String sku = payload.path("sku").asText("");
                    int quantity = payload.path("quantity").asInt(0);
                    if (sku.isEmpty() || sku.length() > 80 || quantity < 1) {
                        return "poison";
                    }
                    Instant expiresAt = Instant.parse(payload.path("expiresAt").asText());
                    UUID eventId = UUID.fromString(event.path("eventId").asText());
                    return store.addReservation(connection, orderId, reservationId, sku, quantity, expiresAt, eventId,
                            now) ? "added" : "duplicate";
                }
                case "inventory.reservation-expired.v1" -> {
                    store.expire(connection, id(payload, "orderId", ORDER_ID),
                            Instant.parse(payload.path("expiredAt").asText()));
                    return "expired";
                }
                case "shipment.confirmed.v1" -> {
                    store.ship(connection, id(payload, "orderId", ORDER_ID),
                            Instant.parse(payload.path("shippedAt").asText()));
                    return "shipped";
                }
                default -> {
                    return "ignored";
                }
            }
        } catch (IllegalArgumentException | DateTimeParseException error) {
            return "poison";
        }
    }

    @Scheduled(every = "1h", delayed = "5m", identity = "pick-list-prune")
    void prune() {
        try {
            int removed = db.inTransaction((Connection c) -> store.prune(c, clock.instant().minus(RETENTION)));
            if (removed > 0) {
                LOG.infof("Pruned %d closed pick list rows", removed);
            }
        } catch (SQLException error) {
            LOG.warnf("Pick list prune failed: %s", error.getMessage());
        }
    }

    private void run() {
        Duration backoff = Duration.ofSeconds(1);
        while (running) {
            try (KafkaConsumer<String, byte[]> created = new KafkaConsumer<>(properties(), new StringDeserializer(),
                    new ByteArrayDeserializer())) {
                consumer = created;
                created.subscribe(List.of(config.kafka().inventoryTopic(), config.kafka().fulfillmentTopic()));
                while (running) {
                    ConsumerRecords<String, byte[]> records = created.poll(Duration.ofSeconds(1));
                    if (!records.isEmpty()) {
                        process(created, records);
                    }
                    backoff = Duration.ofSeconds(1);
                }
            } catch (WakeupException stopping) {
                return;
            } catch (KafkaException | SQLException error) {
                LOG.warnf("Pick list consumer paused (retry in %ds): %s", backoff.toSeconds(), error.getMessage());
                sleep(backoff);
                backoff = backoff.multipliedBy(2).compareTo(Duration.ofSeconds(30)) > 0 ? Duration.ofSeconds(30)
                        : backoff.multipliedBy(2);
            } finally {
                consumer = null;
            }
        }
    }

    private void process(KafkaConsumer<String, byte[]> created, ConsumerRecords<String, byte[]> records)
            throws SQLException {
        Map<String, Integer> outcomes = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> next = new HashMap<>();
        db.inTransaction((Connection connection) -> {
            for (ConsumerRecord<String, byte[]> record : records) {
                outcomes.merge(apply(connection, record.value()), 1, Integer::sum);
                next.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
            }
            return null;
        });
        outcomes.forEach((outcome, count) -> registry.counter("site.picklist.records", "outcome", outcome)
                .increment(count));
        if (outcomes.containsKey("poison")) {
            LOG.warnf("Skipped %d pick list records that could not be parsed", outcomes.get("poison"));
        }
        created.commitSync(next);
    }

    private Properties properties() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka().bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
        properties.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, "10000");
        properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        return properties;
    }

    private static String id(JsonNode payload, String field, Pattern pattern) {
        String value = payload.path(field).asText("");
        if (!pattern.matcher(value).matches()) {
            throw new IllegalArgumentException(field);
        }
        return value;
    }

    private void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }
}
