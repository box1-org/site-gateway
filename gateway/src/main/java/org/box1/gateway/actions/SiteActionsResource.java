package org.box1.gateway.actions;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.box1.gateway.config.SiteConfig;
import org.box1.gateway.outbox.BufferMonitor;
import org.box1.gateway.outbox.BufferStats;
import org.box1.gateway.picklist.PickItem;
import org.box1.gateway.picklist.PickListStore;
import org.box1.gateway.support.Db;

/**
 * The gateway's local API for site-ops. The gateway listens on 127.0.0.1 only (quarkus.http.host), so only processes
 * in the site VM can call it.
 */
@Path("/api/v1/site")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class SiteActionsResource {
    private static final Set<String> STATUSES = Set.of("OPEN", "PICKED", "EXPIRED", "SHIPPED");

    private final SiteActions actions;
    private final PickListStore pickList;
    private final BufferMonitor buffer;
    private final SiteConfig config;
    private final Db db;

    public SiteActionsResource(SiteActions actions, PickListStore pickList, BufferMonitor buffer, SiteConfig config,
            Db db) {
        this.actions = actions;
        this.pickList = pickList;
        this.buffer = buffer;
        this.config = config;
        this.db = db;
    }

    @GET
    @Path("/info")
    public Map<String, Object> info() {
        return Map.of("siteId", config.id(), "instance", config.instance().orElse(""));
    }

    @GET
    @Path("/pick-list")
    public List<PickItem> pickList(@QueryParam("status") String status, @QueryParam("limit") Integer limit)
            throws SQLException {
        if (status != null && !STATUSES.contains(status)) {
            throw new ActionException(400, "STATUS", "status must be one of " + STATUSES);
        }
        int bounded = limit == null ? 100 : Math.max(1, Math.min(limit, 500));
        return db.inTransaction(connection -> pickList.list(connection, Optional.ofNullable(status), bounded));
    }

    @POST
    @Path("/picks")
    public Response pick(PickRequest request) throws SQLException {
        PickRequest body = request == null ? new PickRequest(null, null) : request;
        return created(actions.confirmPick(body.requestId(), body.orderId()));
    }

    @POST
    @Path("/counts")
    public Response count(CountRequest request) throws SQLException {
        CountRequest body = request == null ? new CountRequest(null, null, null) : request;
        return created(actions.recordCount(body.requestId(), body.sku(), body.countedQuantity()));
    }

    @GET
    @Path("/counts")
    public List<ActionResult> counts(@QueryParam("limit") Integer limit) throws SQLException {
        return actions.recent("COUNT", limit == null ? 20 : Math.max(1, Math.min(limit, 200)));
    }

    @POST
    @Path("/holds")
    public Response hold(HoldRequest request) throws SQLException {
        HoldRequest body = request == null ? new HoldRequest(null, null, null, null) : request;
        return created(actions.placeHold(body.requestId(), body.sku(), body.quantity(), body.reasonCode()));
    }

    @POST
    @Path("/holds/{holdId}/release")
    public Response release(@PathParam("holdId") String holdId, ReleaseRequest request) throws SQLException {
        ReleaseRequest body = request == null ? new ReleaseRequest(null) : request;
        return created(actions.releaseHold(body.requestId(), holdId));
    }

    @GET
    @Path("/holds")
    public List<Hold> holds(@QueryParam("status") String status) throws SQLException {
        if (status != null && !Set.of("PLACED", "RELEASED").contains(status)) {
            throw new ActionException(400, "STATUS", "status must be PLACED or RELEASED");
        }
        return actions.holds(Optional.ofNullable(status));
    }

    @GET
    @Path("/buffer")
    public Map<String, Object> buffer() {
        BufferStats stats = buffer.latest();
        return Map.of("readings", stats.readings(), "siteEvents", stats.siteEvents(), "depth", stats.depth(),
                "oldestAgeSeconds", stats.oldestAgeSeconds(),
                "maxReadings", config.buffer().maxReadings(),
                "maxReadingAge", config.buffer().maxReadingAge().toString(),
                "maxSiteEvents", config.buffer().maxSiteEvents());
    }

    private static Response created(ActionResult result) {
        return Response.status(result.replayed() ? 200 : 201).entity(result).build();
    }

    /** Confirm a pick. */
    public record PickRequest(UUID requestId, String orderId) {
    }

    /** Record a cycle count. */
    public record CountRequest(UUID requestId, String sku, Integer countedQuantity) {
    }

    /** Place a hold. */
    public record HoldRequest(UUID requestId, String sku, Integer quantity, String reasonCode) {
    }

    /** Release a hold. */
    public record ReleaseRequest(UUID requestId) {
    }
}
