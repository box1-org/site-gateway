package org.box1.gateway.picklist;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;

/**
 * The site's pick list (box1-docs#129, SITE.md "Pick lists sent to a site"). Every operation is idempotent, so a
 * redelivered or replayed record changes nothing.
 */
@ApplicationScoped
public class PickListStore {
    static final String COLUMNS = "order_id, reservation_id, sku, quantity, expires_at, status, received_at, pick_id, "
            + "picked_at, closed_at";

    /** Adds a reservation for this site, closed at once if its closing event arrived first. */
    public boolean addReservation(Connection connection, String orderId, String reservationId, String sku, int quantity,
            Instant expiresAt, UUID reservedEventId, Instant now) throws SQLException {
        String status = "OPEN";
        Timestamp closedAt = null;
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT reason, closed_at FROM pick_closure WHERE order_id = ?")) {
            statement.setString(1, orderId);
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    status = result.getString(1);
                    closedAt = result.getTimestamp(2);
                }
            }
        }
        String sql = "INSERT INTO pick_item (order_id, reservation_id, sku, quantity, expires_at, reserved_event_id, "
                + "status, received_at, closed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (order_id) DO NOTHING";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, orderId);
            statement.setString(2, reservationId);
            statement.setString(3, sku);
            statement.setInt(4, quantity);
            statement.setTimestamp(5, Timestamp.from(expiresAt));
            statement.setObject(6, reservedEventId);
            statement.setString(7, status);
            statement.setTimestamp(8, Timestamp.from(now));
            statement.setTimestamp(9, closedAt);
            return statement.executeUpdate() == 1;
        }
    }

    /** An open item expires. A picked item stays picked: its units were already moved. */
    public void expire(Connection connection, String orderId, Instant at) throws SQLException {
        if (!exists(connection, orderId)) {
            remember(connection, orderId, "EXPIRED", at);
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE pick_item SET status = 'EXPIRED', closed_at = ? WHERE order_id = ? AND status = 'OPEN'")) {
            statement.setTimestamp(1, Timestamp.from(at));
            statement.setString(2, orderId);
            statement.executeUpdate();
        }
    }

    /** A confirmed shipment closes the item whatever its state. */
    public void ship(Connection connection, String orderId, Instant at) throws SQLException {
        if (!exists(connection, orderId)) {
            remember(connection, orderId, "SHIPPED", at);
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE pick_item SET status = 'SHIPPED', closed_at = ? WHERE order_id = ? AND status <> 'SHIPPED'")) {
            statement.setTimestamp(1, Timestamp.from(at));
            statement.setString(2, orderId);
            statement.executeUpdate();
        }
    }

    public List<PickItem> list(Connection connection, Optional<String> status, int limit) throws SQLException {
        String sql = "SELECT " + COLUMNS + " FROM pick_item " + (status.isPresent() ? "WHERE status = ? " : "")
                + "ORDER BY received_at, order_id LIMIT ?";
        List<PickItem> items = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int index = 1;
            if (status.isPresent()) {
                statement.setString(index++, status.get());
            }
            statement.setInt(index, limit);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    items.add(read(result));
                }
            }
        }
        return items;
    }

    /** Locks one item for an operator action. */
    public Optional<LockedItem> lock(Connection connection, String orderId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT " + COLUMNS + ", reserved_event_id FROM pick_item WHERE order_id = ? FOR UPDATE")) {
            statement.setString(1, orderId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                return Optional.of(new LockedItem(read(result), result.getObject(11, UUID.class)));
            }
        }
    }

    public void markPicked(Connection connection, String orderId, String pickId, Instant at) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "UPDATE pick_item SET status = 'PICKED', pick_id = ?, picked_at = ? WHERE order_id = ? AND status = 'OPEN'")) {
            statement.setString(1, pickId);
            statement.setTimestamp(2, Timestamp.from(at));
            statement.setString(3, orderId);
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Pick list item " + orderId + " is no longer open");
            }
        }
    }

    public long countOpen(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT count(*) FROM pick_item WHERE status = 'OPEN'");
                ResultSet result = statement.executeQuery()) {
            result.next();
            return result.getLong(1);
        }
    }

    /** Removes closed items and remembered closures older than the cutoff (longer than the topics' retention). */
    public int prune(Connection connection, Instant cutoff) throws SQLException {
        int removed;
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM pick_item WHERE status IN ('EXPIRED', 'SHIPPED') AND closed_at < ?")) {
            statement.setTimestamp(1, Timestamp.from(cutoff));
            removed = statement.executeUpdate();
        }
        try (PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM pick_closure WHERE received_at < ?")) {
            statement.setTimestamp(1, Timestamp.from(cutoff));
            removed += statement.executeUpdate();
        }
        return removed;
    }

    private static boolean exists(Connection connection, String orderId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT 1 FROM pick_item WHERE order_id = ?")) {
            statement.setString(1, orderId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    /** Remembers a closure for a reservation not seen yet. SHIPPED is final and wins over EXPIRED. */
    private static void remember(Connection connection, String orderId, String reason, Instant at) throws SQLException {
        String sql = "INSERT INTO pick_closure (order_id, reason, closed_at) VALUES (?, ?, ?) "
                + "ON CONFLICT (order_id) DO UPDATE SET reason = 'SHIPPED', closed_at = EXCLUDED.closed_at "
                + "WHERE EXCLUDED.reason = 'SHIPPED' AND pick_closure.reason <> 'SHIPPED'";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, orderId);
            statement.setString(2, reason);
            statement.setTimestamp(3, Timestamp.from(at));
            statement.executeUpdate();
        }
    }

    private static PickItem read(ResultSet result) throws SQLException {
        return new PickItem(result.getString(1), result.getString(2), result.getString(3), result.getInt(4),
                instant(result.getTimestamp(5)), result.getString(6), instant(result.getTimestamp(7)),
                result.getString(8), instant(result.getTimestamp(9)), instant(result.getTimestamp(10)));
    }

    private static Instant instant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    /** A pick list item locked for update, with the eventId of the reservation that created it. */
    public record LockedItem(PickItem item, UUID reservedEventId) {
    }
}
