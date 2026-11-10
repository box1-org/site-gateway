package org.box1.ops;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/** The gateway's local API (127.0.0.1:8080). site-ops keeps no state of its own. */
@RegisterRestClient(configKey = "gateway")
@Path("/api/v1/site")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface GatewayClient {
    @GET
    @Path("/info")
    Map<String, Object> info();

    @GET
    @Path("/pick-list")
    List<PickItem> pickList(@QueryParam("status") String status, @QueryParam("limit") Integer limit);

    @POST
    @Path("/picks")
    ActionResult pick(Map<String, Object> request);

    @POST
    @Path("/counts")
    ActionResult count(Map<String, Object> request);

    @GET
    @Path("/counts")
    List<ActionResult> counts(@QueryParam("limit") Integer limit);

    @POST
    @Path("/holds")
    ActionResult hold(Map<String, Object> request);

    @POST
    @Path("/holds/{holdId}/release")
    ActionResult release(@PathParam("holdId") String holdId, Map<String, Object> request);

    @GET
    @Path("/holds")
    List<Hold> holds(@QueryParam("status") String status);

    @GET
    @Path("/buffer")
    Map<String, Object> buffer();

    /** A pick list item. */
    record PickItem(String orderId, String reservationId, String sku, int quantity, Instant expiresAt, String status,
            Instant receivedAt, String pickId, Instant pickedAt, Instant closedAt) {
    }

    /** A recorded operator action. */
    record ActionResult(String action, String localId, UUID eventId, String orderId, String sku, int quantity,
            Instant recordedAt, boolean replayed) {
    }

    /** A hold. */
    record Hold(String holdId, String sku, int quantity, String reasonCode, String status, Instant placedAt,
            Instant releasedAt) {
    }
}
