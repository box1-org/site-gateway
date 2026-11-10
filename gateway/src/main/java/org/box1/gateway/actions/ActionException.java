package org.box1.gateway.actions;

/** An operator action that cannot be recorded, with the HTTP status and a stable code. */
public class ActionException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int status;
    private final String code;

    public ActionException(int status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public int status() {
        return status;
    }

    public String code() {
        return code;
    }
}
