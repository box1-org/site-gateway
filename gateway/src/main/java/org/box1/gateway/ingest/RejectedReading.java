package org.box1.gateway.ingest;

/** A message that can never become a valid reading. It is counted, logged and acknowledged, never retried. */
public class RejectedReading extends Exception {
    private static final long serialVersionUID = 1L;
    private final String reason;

    public RejectedReading(String reason) {
        super(reason);
        this.reason = reason;
    }

    public String reason() {
        return reason;
    }
}
