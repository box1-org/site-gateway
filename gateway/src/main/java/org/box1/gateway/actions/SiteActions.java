package org.box1.gateway.actions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import jakarta.enterprise.context.ApplicationScoped;
import org.box1.gateway.config.SiteConfig;
import org.box1.gateway.outbox.BufferFullException;
import org.box1.gateway.outbox.Envelopes;
import org.box1.gateway.outbox.EventClass;
import org.box1.gateway.outbox.OutboxStore;
import org.box1.gateway.outbox.SiteEvents;
import org.box1.gateway.picklist.PickItem;
import org.box1.gateway.picklist.PickListStore;
import org.box1.gateway.support.Db;

/**
 * Operator actions from site-ops (box1-docs#129): each one changes the site's local state and writes its site event
 * to the outbox in the same transaction, so it works offline and is published exactly as recorded. requestId makes a
 * repeated submit return the first result instead of recording twice.
 */
@ApplicationScoped
public class SiteActions {
    public static final Set<String> HOLD_REASONS = Set.of("DAMAGED", "QUALITY_CHECK", "COUNT_DISCREPANCY", "OTHER");
    private static final Pattern ORDER_ID = Pattern.compile("^ord_[A-Za-z0-9_-]+$");
    private static final Pattern HOLD_ID = Pattern.compile("^hold_[a-f0-9]{32}$");
    private static final int MAX_QUANTITY = 1_000_000;

    private final Db db;
    private final OutboxStore outbox;
    private final PickListStore pickList;
    private final SiteConfig config;
    private final Clock clock;

    public SiteActions(Db db, OutboxStore outbox, PickListStore pickList, SiteConfig config, Clock clock) {
        this.db = db;
        this.outbox = outbox;
        this.pickList = pickList;
        this.config = config;
        this.clock = clock;
    }

    /** Confirms that one open pick list item's reserved units were picked (the whole reserved quantity). */
    public ActionResult confirmPick(UUID requestId, String orderId) throws SQLException {
        require(requestId != null, "REQUEST_ID", "requestId is required");
        require(orderId != null && ORDER_ID.matcher(orderId).matches(), "ORDER_ID", "orderId is invalid");
        return record(requestId, "PICK", connection -> {
            PickListStore.LockedItem locked = pickList.lock(connection, orderId)
                    .orElseThrow(() -> new ActionException(404, "NOT_ON_PICK_LIST", orderId + " is not on the pick list"));
            PickItem item = locked.item();
            if (!"OPEN".equals(item.status())) {
                throw new ActionException(409, "NOT_OPEN", orderId + " is " + item.status());
            }
            Instant now = clock.instant();
            String pickId = "pick_" + hex(UUID.randomUUID());
            UUID eventId = UUID.randomUUID();
            pickList.markPicked(connection, orderId, pickId, now);
            outbox.addSiteEvent(connection, eventId, Envelopes.PICK_CONFIRMED, now, SiteEvents.pickConfirmed(eventId,
                    config.id(), pickId, item.orderId(), item.reservationId(), item.sku(), item.quantity(), now,
                    locked.reservedEventId(), traceparent()));
            return new ActionResult("PICK", pickId, eventId, item.orderId(), item.sku(), item.quantity(), now, false);
        });
    }

    /** Records a cycle count: the units of one SKU physically at the site now. */
    public ActionResult recordCount(UUID requestId, String sku, Integer countedQuantity) throws SQLException {
        require(requestId != null, "REQUEST_ID", "requestId is required");
        requireSku(sku);
        require(countedQuantity != null && countedQuantity >= 0 && countedQuantity <= MAX_QUANTITY, "QUANTITY",
                "countedQuantity must be 0.." + MAX_QUANTITY);
        return record(requestId, "COUNT", connection -> {
            Instant now = clock.instant();
            String countId = "cnt_" + hex(UUID.randomUUID());
            UUID eventId = UUID.randomUUID();
            outbox.addSiteEvent(connection, eventId, Envelopes.STOCK_ADJUSTED, now, SiteEvents.stockAdjusted(eventId,
                    config.id(), countId, sku, countedQuantity, now, traceparent()));
            return new ActionResult("COUNT", countId, eventId, null, sku, countedQuantity, now, false);
        });
    }

    /** Places a hold on units of one SKU. In v1 holds are site-local and audited centrally (SITE.md). */
    public ActionResult placeHold(UUID requestId, String sku, Integer quantity, String reasonCode) throws SQLException {
        require(requestId != null, "REQUEST_ID", "requestId is required");
        requireSku(sku);
        require(quantity != null && quantity >= 1 && quantity <= MAX_QUANTITY, "QUANTITY",
                "quantity must be 1.." + MAX_QUANTITY);
        require(reasonCode != null && HOLD_REASONS.contains(reasonCode), "REASON", "reasonCode must be one of "
                + HOLD_REASONS);
        return record(requestId, "HOLD_PLACED", connection -> {
            Instant now = clock.instant();
            String holdId = "hold_" + hex(UUID.randomUUID());
            try (PreparedStatement statement = connection.prepareStatement("INSERT INTO site_hold (hold_id, sku, "
                    + "quantity, reason_code, status, placed_at) VALUES (?, ?, ?, ?, 'PLACED', ?)")) {
                statement.setString(1, holdId);
                statement.setString(2, sku);
                statement.setInt(3, quantity);
                statement.setString(4, reasonCode);
                statement.setTimestamp(5, Timestamp.from(now));
                statement.executeUpdate();
            }
            UUID eventId = holdEvent(connection, holdId, sku, quantity, "PLACED", reasonCode, now);
            return new ActionResult("HOLD_PLACED", holdId, eventId, null, sku, quantity, now, false);
        });
    }

    /** Releases a placed hold. */
    public ActionResult releaseHold(UUID requestId, String holdId) throws SQLException {
        require(requestId != null, "REQUEST_ID", "requestId is required");
        require(holdId != null && HOLD_ID.matcher(holdId).matches(), "HOLD_ID", "holdId is invalid");
        return record(requestId, "HOLD_RELEASED", connection -> {
            Hold hold = lockHold(connection, holdId)
                    .orElseThrow(() -> new ActionException(404, "UNKNOWN_HOLD", holdId + " does not exist"));
            if (!"PLACED".equals(hold.status())) {
                throw new ActionException(409, "NOT_PLACED", holdId + " is " + hold.status());
            }
            Instant now = clock.instant();
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE site_hold SET status = 'RELEASED', released_at = ? WHERE hold_id = ?")) {
                statement.setTimestamp(1, Timestamp.from(now));
                statement.setString(2, holdId);
                statement.executeUpdate();
            }
            UUID eventId = holdEvent(connection, holdId, hold.sku(), hold.quantity(), "RELEASED", hold.reasonCode(), now);
            return new ActionResult("HOLD_RELEASED", holdId, eventId, null, hold.sku(), hold.quantity(), now, false);
        });
    }

    public List<Hold> holds(Optional<String> status) throws SQLException {
        String sql = "SELECT hold_id, sku, quantity, reason_code, status, placed_at, released_at FROM site_hold "
                + (status.isPresent() ? "WHERE status = ? " : "") + "ORDER BY placed_at DESC LIMIT 200";
        return db.inTransaction(connection -> {
            List<Hold> holds = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                if (status.isPresent()) {
                    statement.setString(1, status.get());
                }
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        holds.add(hold(result));
                    }
                }
            }
            return holds;
        });
    }

    /** The most recent actions of one kind, newest first. */
    public List<ActionResult> recent(String action, int limit) throws SQLException {
        return db.inTransaction(connection -> {
            List<ActionResult> results = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement("SELECT action, local_id, event_id, "
                    + "order_id, sku, quantity, recorded_at FROM site_action WHERE action = ? "
                    + "ORDER BY recorded_at DESC LIMIT ?")) {
                statement.setString(1, action);
                statement.setInt(2, limit);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        results.add(action(result, false));
                    }
                }
            }
            return results;
        });
    }

    private ActionResult record(UUID requestId, String action, Db.Work<ActionResult> work) throws SQLException {
        ActionResult recorded = recordOnce(requestId, action, work);
        if (!recorded.replayed()) {
            outbox.countRecorded(EventClass.SITE_EVENT);
        }
        return recorded;
    }

    private ActionResult recordOnce(UUID requestId, String action, Db.Work<ActionResult> work) throws SQLException {
        try {
            return db.inTransaction(connection -> {
                // Serialise submits of the same requestId: a concurrent duplicate waits here, then sees the first
                // one's result instead of failing on the state the first one changed.
                try (PreparedStatement lock = connection.prepareStatement(
                        "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))")) {
                    lock.setString(1, requestId.toString());
                    lock.execute();
                }
                Optional<ActionResult> earlier = earlier(connection, requestId);
                if (earlier.isPresent()) {
                    if (!earlier.get().action().equals(action)) {
                        throw new ActionException(409, "REQUEST_ID_REUSED", "requestId was used for another action");
                    }
                    return earlier.get();
                }
                ActionResult result = work.run(connection);
                try (PreparedStatement statement = connection.prepareStatement("INSERT INTO site_action (request_id, "
                        + "action, local_id, event_id, order_id, sku, quantity, recorded_at) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
                    statement.setObject(1, requestId);
                    statement.setString(2, action);
                    statement.setString(3, result.localId());
                    statement.setObject(4, result.eventId());
                    statement.setString(5, result.orderId());
                    statement.setString(6, result.sku());
                    statement.setInt(7, result.quantity());
                    statement.setTimestamp(8, Timestamp.from(result.recordedAt()));
                    statement.executeUpdate();
                }
                return result;
            });
        } catch (BufferFullException full) {
            throw new ActionException(503, "BUFFER_FULL", full.getMessage());
        } catch (SQLException error) {
            // Two submits of the same requestId raced: the second hits the primary key. Return the first.
            if ("23505".equals(error.getSQLState())) {
                Optional<ActionResult> earlier = db.inTransaction(connection -> earlier(connection, requestId));
                if (earlier.isPresent() && earlier.get().action().equals(action)) {
                    return earlier.get();
                }
            }
            throw error;
        }
    }

    private static Optional<ActionResult> earlier(Connection connection, UUID requestId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT action, local_id, event_id, order_id, "
                + "sku, quantity, recorded_at FROM site_action WHERE request_id = ?")) {
            statement.setObject(1, requestId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(action(result, true)) : Optional.empty();
            }
        }
    }

    private UUID holdEvent(Connection connection, String holdId, String sku, int quantity, String action,
            String reasonCode, Instant now) throws SQLException {
        UUID eventId = UUID.randomUUID();
        outbox.addSiteEvent(connection, eventId, Envelopes.STOCK_HELD, now, SiteEvents.stockHeld(eventId, config.id(),
                holdId, sku, quantity, action, reasonCode, now, traceparent()));
        return eventId;
    }

    private static Optional<Hold> lockHold(Connection connection, String holdId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT hold_id, sku, quantity, reason_code, "
                + "status, placed_at, released_at FROM site_hold WHERE hold_id = ? FOR UPDATE")) {
            statement.setString(1, holdId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.of(hold(result)) : Optional.empty();
            }
        }
    }

    private static Hold hold(ResultSet result) throws SQLException {
        Timestamp released = result.getTimestamp(7);
        return new Hold(result.getString(1), result.getString(2), result.getInt(3), result.getString(4),
                result.getString(5), result.getTimestamp(6).toInstant(), released == null ? null : released.toInstant());
    }

    private static ActionResult action(ResultSet result, boolean replayed) throws SQLException {
        return new ActionResult(result.getString(1), result.getString(2), result.getObject(3, UUID.class),
                result.getString(4), result.getString(5), result.getInt(6), result.getTimestamp(7).toInstant(), replayed);
    }

    private static void requireSku(String sku) {
        require(sku != null && !sku.isBlank() && sku.length() <= 80 && sku.equals(sku.strip()), "SKU",
                "sku must be 1..80 characters without surrounding spaces");
    }

    private static void require(boolean condition, String code, String message) {
        if (!condition) {
            throw new ActionException(400, code, message);
        }
    }

    private static String hex(UUID uuid) {
        return uuid.toString().replace("-", "");
    }

    private static String traceparent() {
        SpanContext context = Span.current().getSpanContext();
        if (!context.isValid()) {
            return null;
        }
        return "00-" + context.getTraceId() + "-" + context.getSpanId() + "-" + context.getTraceFlags().asHex();
    }
}
