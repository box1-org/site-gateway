package org.box1.gateway.outbox;

/**
 * The outbox as the site agent and metrics report it.
 *
 * @param readings unpublished readings
 * @param siteEvents unpublished site events
 * @param oldestAgeSeconds age of the oldest unpublished row, 0 when empty
 */
public record BufferStats(long readings, long siteEvents, long oldestAgeSeconds) {
    public static final BufferStats EMPTY = new BufferStats(0, 0, 0);

    public long depth() {
        return readings + siteEvents;
    }
}
