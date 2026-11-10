package org.box1.gateway.outbox;

/** The site-event backlog is at its bound: the operator action is refused, never dropped (backpressure). */
public class BufferFullException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public BufferFullException(long backlog, int limit) {
        super("Site event backlog " + backlog + " is at its limit " + limit + "; retry when the site reconnects");
    }
}
