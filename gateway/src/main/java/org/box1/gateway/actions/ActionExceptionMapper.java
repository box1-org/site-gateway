package org.box1.gateway.actions;

import java.util.Map;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/** Maps refused operator actions to JSON errors with a stable code. */
@Provider
public class ActionExceptionMapper implements ExceptionMapper<ActionException> {
    @Override
    public Response toResponse(ActionException error) {
        return Response.status(error.status()).type(MediaType.APPLICATION_JSON)
                .entity(Map.of("code", error.code(), "message", error.getMessage())).build();
    }
}
