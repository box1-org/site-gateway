package org.box1.gateway.publish;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.box1.gateway.config.SiteConfig;
import org.box1.gateway.outbox.OutboxRow;
import org.box1.gateway.outbox.OutboxStore;
import org.box1.gateway.support.Db;
import org.jboss.logging.Logger;

/**
 * Drains the outbox to Kafka (box1-docs#127). One publisher per site; rows are sent in seq order and marked published
 * only after the broker acknowledged them, as the longest acknowledged prefix of each batch. A crash between the
 * acknowledgement and the commit re-sends rows with the same eventId, which consumers deduplicate. While Kafka is
 * unreachable the publisher backs off exponentially up to one minute; nothing is dropped here.
 */
@ApplicationScoped
public class OutboxPublisher {
    private static final Logger LOG = Logger.getLogger(OutboxPublisher.class);
    private static final Duration MAX_BACKOFF = Duration.ofMinutes(1);
    private static final long ACK_WAIT_SECONDS = 45;

    private final Db db;
    private final OutboxStore store;
    private final KafkaSender sender;
    private final SiteConfig config;
    private final Clock clock;
    private final MeterRegistry registry;
    private Duration backoff = Duration.ZERO;
    private Instant nextAttempt = Instant.MIN;
    private boolean failing;

    public OutboxPublisher(Db db, OutboxStore store, KafkaSender sender, SiteConfig config, Clock clock,
            MeterRegistry registry) {
        this.db = db;
        this.store = store;
        this.sender = sender;
        this.config = config;
        this.clock = clock;
        this.registry = registry;
    }

    @Scheduled(every = "${gateway.publisher.interval:1s}", concurrentExecution = Scheduled.ConcurrentExecution.SKIP,
            identity = "outbox-publisher", skipExecutionIf = PublisherDisabled.class)
    void tick() {
        if (clock.instant().isBefore(nextAttempt)) {
            return;
        }
        try {
            int published;
            do {
                published = drainOnce();
            } while (published == config.buffer().publishBatch());
            if (failing) {
                LOG.info("Kafka publishing recovered");
            }
            failing = false;
            backoff = Duration.ZERO;
        } catch (SQLException error) {
            backoff = backoff.isZero() ? Duration.ofSeconds(1) : min(backoff.multipliedBy(2), MAX_BACKOFF);
            nextAttempt = clock.instant().plus(backoff);
            if (!failing) {
                LOG.warnf("Kafka publishing paused, retrying with backoff up to %ds: %s", MAX_BACKOFF.toSeconds(),
                        error.getMessage());
            }
            failing = true;
        }
    }

    /** Sends one batch and marks its acknowledged prefix published (also used by tests). */
    public int drainOnce() throws SQLException {
        return db.inTransaction((Connection connection) -> {
            List<OutboxRow> rows = store.lockPending(connection, config.buffer().publishBatch());
            if (rows.isEmpty()) {
                return 0;
            }
            List<Future<RecordMetadata>> futures = new ArrayList<>(rows.size());
            PublishException failure = null;
            try {
                for (OutboxRow row : rows) {
                    futures.add(sender.send(row));
                }
            } catch (RuntimeException error) {
                failure = new PublishException(error);
            }
            List<OutboxRow> acknowledged = new ArrayList<>(futures.size());
            for (int i = 0; i < futures.size() && failure == null; i++) {
                try {
                    futures.get(i).get(ACK_WAIT_SECONDS, TimeUnit.SECONDS);
                    acknowledged.add(rows.get(i));
                } catch (ExecutionException | TimeoutException error) {
                    failure = new PublishException(error);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    failure = new PublishException(error);
                }
            }
            store.markPublished(connection, acknowledged);
            for (OutboxRow row : acknowledged) {
                registry.counter("site.outbox.published", "class", row.eventClass().name().toLowerCase(Locale.ROOT))
                        .increment();
            }
            if (failure != null) {
                // Commit the acknowledged prefix, then fail the tick so the rest is retried after a backoff.
                connection.commit();
                sender.reset();
                throw failure;
            }
            return acknowledged.size();
        });
    }

    private static Duration min(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    /** A send that the broker did not acknowledge. */
    static final class PublishException extends SQLException {
        private static final long serialVersionUID = 1L;

        PublishException(Throwable cause) {
            super(cause.getMessage(), cause);
        }
    }

    /** Skips the scheduled publisher when it is turned off (tests, or a site that must not publish). */
    @ApplicationScoped
    public static class PublisherDisabled implements Scheduled.SkipPredicate {
        private final SiteConfig config;

        public PublisherDisabled(SiteConfig config) {
            this.config = config;
        }

        @Override
        public boolean test(io.quarkus.scheduler.ScheduledExecution execution) {
            return !config.kafka().publisherEnabled();
        }
    }
}
