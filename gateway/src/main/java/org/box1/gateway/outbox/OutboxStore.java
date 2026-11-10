package org.box1.gateway.outbox;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.gateway.config.SiteConfig;
import org.jboss.logging.Logger;

/**
 * The site outbox (box1-docs#127). Inserts happen inside the caller's transaction, together with the local effect.
 * The bound is enforced on insert (size, readings only) and by {@link BufferGuard} (age, pruning).
 */
@ApplicationScoped
public class OutboxStore {
    /** The outcome of adding a reading: whether it was new, and how many older readings the bound dropped. */
    public record ReadingInsert(boolean added, long dropped) {
    }

    private static final Logger LOG = Logger.getLogger(OutboxStore.class);

    private final ObjectMapper mapper;
    private final SiteConfig config;
    private final Clock clock;
    private final MeterRegistry registry;

    public OutboxStore(ObjectMapper mapper, SiteConfig config, Clock clock, MeterRegistry registry) {
        this.mapper = mapper;
        this.config = config;
        this.clock = clock;
        this.registry = registry;
    }

    /**
     * Adds a reading. {@code added} is false when a row with this eventId already exists (an MQTT redelivery). When the
     * readings bound is reached, the oldest unpublished readings are dropped first and the drop is recorded. Metrics
     * are the caller's, after the transaction commits ({@link #countRecorded}, {@link #countDropped}).
     */
    public ReadingInsert addReading(Connection connection, UUID eventId, String assetId, Instant occurredAt,
            Instant measuredAt, Map<String, Object> envelope) throws SQLException {
        boolean added = insert(connection, eventId, EventClass.READING, Envelopes.READING_EVENT,
                config.kafka().readingsTopic(), assetId, occurredAt, measuredAt, envelope);
        long dropped = 0;
        if (added) {
            long pending = countPending(connection, EventClass.READING);
            long excess = pending - config.buffer().maxReadings();
            if (excess > 0) {
                dropped = dropOldestReadings(connection, excess, eventId);
            }
        }
        return new ReadingInsert(added, dropped);
    }

    public void countRecorded(EventClass eventClass) {
        registry.counter("site.outbox.recorded", "class", tag(eventClass)).increment();
    }

    public void countPublished(EventClass eventClass, long rows) {
        registry.counter("site.outbox.published", "class", tag(eventClass)).increment(rows);
    }

    public void countDropped(String reason, long rows) {
        if (rows > 0) {
            registry.counter("site.outbox.dropped", "reason", reason).increment(rows);
        }
    }

    private static String tag(EventClass eventClass) {
        return eventClass.name().toLowerCase(java.util.Locale.ROOT);
    }

    /**
     * Adds a site event. Refuses with {@link BufferFullException} when the site-event backlog is at its bound, so the
     * operator's action is not recorded rather than recorded and later lost.
     */
    public void addSiteEvent(Connection connection, UUID eventId, String eventType, Instant occurredAt,
            Map<String, Object> envelope) throws SQLException {
        long backlog = countPending(connection, EventClass.SITE_EVENT);
        if (backlog >= config.buffer().maxSiteEvents()) {
            registry.counter("site.outbox.refused").increment();
            throw new BufferFullException(backlog, config.buffer().maxSiteEvents());
        }
        if (!insert(connection, eventId, EventClass.SITE_EVENT, eventType, config.kafka().siteTopic(), config.id(),
                occurredAt, null, envelope)) {
            throw new IllegalStateException("Duplicate site event " + eventId);
        }
    }

    /** Locks up to {@code limit} unpublished rows in seq order, skipping rows another transaction holds. */
    public List<OutboxRow> lockPending(Connection connection, int limit) throws SQLException {
        String sql = "SELECT seq, event_id, event_class, topic, record_key, envelope FROM outbox "
                + "WHERE published_at IS NULL ORDER BY seq LIMIT ? FOR UPDATE SKIP LOCKED";
        List<OutboxRow> rows = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, limit);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    rows.add(new OutboxRow(result.getLong(1), result.getObject(2, UUID.class),
                            EventClass.valueOf(result.getString(3)), result.getString(4), result.getString(5),
                            result.getString(6)));
                }
            }
        }
        return rows;
    }

    public void markPublished(Connection connection, List<OutboxRow> rows) throws SQLException {
        if (rows.isEmpty()) {
            return;
        }
        Long[] seqs = rows.stream().map(OutboxRow::seq).toArray(Long[]::new);
        Array array = connection.createArrayOf("bigint", seqs);
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE outbox SET published_at = ?, attempts = attempts + 1 WHERE seq = ANY(?)")) {
            statement.setTimestamp(1, Timestamp.from(clock.instant()));
            statement.setArray(2, array);
            statement.executeUpdate();
        } finally {
            array.free();
        }
    }

    public BufferStats stats(Connection connection) throws SQLException {
        String sql = "SELECT count(*) FILTER (WHERE event_class = 'READING'), "
                + "count(*) FILTER (WHERE event_class = 'SITE_EVENT'), min(created_at) "
                + "FROM outbox WHERE published_at IS NULL";
        try (PreparedStatement statement = connection.prepareStatement(sql);
                ResultSet result = statement.executeQuery()) {
            result.next();
            Timestamp oldest = result.getTimestamp(3);
            long age = oldest == null ? 0
                    : Math.max(0, Duration.between(oldest.toInstant(), clock.instant()).toSeconds());
            return new BufferStats(result.getLong(1), result.getLong(2), age);
        }
    }

    /**
     * Drops unpublished readings older than the age bound, measured from the older of measuredAt and the time the site
     * received them (a reading already old on arrival, for example held in the broker, ages from its measurement).
     * Returns the number dropped.
     */
    public long dropExpiredReadings(Connection connection) throws SQLException {
        Instant cutoff = clock.instant().minus(config.buffer().maxReadingAge());
        String sql = "DELETE FROM outbox WHERE seq IN (SELECT seq FROM outbox WHERE published_at IS NULL "
                + "AND event_class = 'READING' AND LEAST(COALESCE(measured_at, occurred_at), occurred_at) < ? "
                + "ORDER BY seq LIMIT 10000 FOR UPDATE SKIP LOCKED) RETURNING seq";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(cutoff));
            return recordDrop(connection, statement, "AGE");
        }
    }

    /** Deletes published rows older than the retention. Returns the number deleted. */
    public int prunePublished(Connection connection) throws SQLException {
        Instant cutoff = clock.instant().minus(config.buffer().publishedRetention());
        String sql = "DELETE FROM outbox WHERE seq IN (SELECT seq FROM outbox WHERE published_at IS NOT NULL "
                + "AND published_at < ? ORDER BY seq LIMIT 10000)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(cutoff));
            return statement.executeUpdate();
        }
    }

    private boolean insert(Connection connection, UUID eventId, EventClass eventClass, String eventType, String topic,
            String key, Instant occurredAt, Instant measuredAt, Map<String, Object> envelope) throws SQLException {
        String sql = "INSERT INTO outbox (event_id, event_class, event_type, topic, record_key, envelope, occurred_at, "
                + "created_at, measured_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, eventId);
            statement.setString(2, eventClass.name());
            statement.setString(3, eventType);
            statement.setString(4, topic);
            statement.setString(5, key);
            statement.setString(6, json(envelope));
            statement.setTimestamp(7, Timestamp.from(occurredAt));
            statement.setTimestamp(8, Timestamp.from(clock.instant()));
            statement.setTimestamp(9, measuredAt == null ? null : Timestamp.from(measuredAt));
            return statement.executeUpdate() == 1;
        }
    }

    private long countPending(Connection connection, EventClass eventClass) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT count(*) FROM outbox WHERE published_at IS NULL AND event_class = ?")) {
            statement.setString(1, eventClass.name());
            try (ResultSet result = statement.executeQuery()) {
                result.next();
                return result.getLong(1);
            }
        }
    }

    /** Drops the oldest unpublished readings, never the one just inserted; rows a publisher holds are skipped. */
    private long dropOldestReadings(Connection connection, long count, UUID keep) throws SQLException {
        String sql = "DELETE FROM outbox WHERE seq IN (SELECT seq FROM outbox WHERE published_at IS NULL "
                + "AND event_class = 'READING' AND event_id <> ? ORDER BY seq LIMIT ? FOR UPDATE SKIP LOCKED) RETURNING seq";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, keep);
            statement.setLong(2, count);
            return recordDrop(connection, statement, "SIZE");
        }
    }

    private long recordDrop(Connection connection, PreparedStatement delete, String reason) throws SQLException {
        long dropped = 0;
        long first = Long.MAX_VALUE;
        long last = Long.MIN_VALUE;
        try (ResultSet result = delete.executeQuery()) {
            while (result.next()) {
                long seq = result.getLong(1);
                dropped++;
                first = Math.min(first, seq);
                last = Math.max(last, seq);
            }
        }
        if (dropped > 0) {
            try (PreparedStatement log = connection.prepareStatement(
                    "INSERT INTO outbox_drop (dropped_at, reason, rows_dropped, first_seq, last_seq) VALUES (?, ?, ?, ?, ?)")) {
                log.setTimestamp(1, Timestamp.from(clock.instant()));
                log.setString(2, reason);
                log.setInt(3, Math.toIntExact(dropped));
                log.setLong(4, first);
                log.setLong(5, last);
                log.executeUpdate();
            }
            LOG.warnf("Dropped %d unpublished readings (bound %s), seq %d..%d", dropped, reason, first, last);
        }
        return dropped;
    }

    private String json(Map<String, Object> envelope) {
        try {
            return mapper.writeValueAsString(envelope);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Event serialization failed", error);
        }
    }
}
