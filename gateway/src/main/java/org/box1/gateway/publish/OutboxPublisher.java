package org.box1.gateway.publish;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.box1.gateway.config.SiteConfig;
import org.box1.gateway.outbox.EventClass;
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
        List<OutboxRow> published = db.inTransaction((Connection connection) -> {
            List<OutboxRow> rows = store.lockPending(connection, config.buffer().publishBatch());
            if (rows.isEmpty()) {
                return List.<OutboxRow>of();
            }
            List<Future<RecordMetadata>> futures = new ArrayList<>(rows.size());
            PublishException failure = null;
            try {
                // One bounded wait when the cluster is unreachable, instead of one per row while the rows are locked.
                sender.requireMetadata(rows.stream().map(OutboxRow::topic).distinct().toList());
                for (OutboxRow row : rows) {
                    Future<RecordMetadata> future = sender.send(row);
                    futures.add(future);
                    if (future.isDone() && future.state() == Future.State.FAILED) {
                        break;
                    }
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
            if (failure != null) {
                // Commit the acknowledged prefix, then fail the tick so the rest is retried after a backoff.
                connection.commit();
                count(acknowledged);
                if (!retriable(failure)) {
                    sender.reset();
                }
                throw failure;
            }
            return acknowledged;
        });
        count(published);
        return published.size();
    }

    private void count(List<OutboxRow> rows) {
        for (EventClass eventClass : EventClass.values()) {
            store.countPublished(eventClass, rows.stream().filter(r -> r.eventClass() == eventClass).count());
        }
    }

    /** Timeouts and other retriable errors keep the producer (and its metadata); anything else replaces it. */
    static boolean retriable(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof org.apache.kafka.common.errors.RetriableException
                    || cause instanceof TimeoutException) {
                return true;
            }
        }
        return false;
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
