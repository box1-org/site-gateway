package org.box1.ops;

import io.quarkus.qute.TemplateExtension;

/** Template access to a fresh requestId per rendered form: {ops:requestId}. */
@TemplateExtension(namespace = "ops")
public final class RequestIds {
    private RequestIds() {
    }

    static String requestId() {
        return OpsPages.newRequestId();
    }
}
