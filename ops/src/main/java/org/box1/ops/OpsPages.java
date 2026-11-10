package org.box1.ops;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

/**
 * site-ops handheld pages (box1-docs#129): the pick list, cycle counts and holds. Pages are server-rendered with Qute;
 * htmx swaps fragments. Every form carries a fresh requestId, so a double tap records one action. Works while the site
 * is offline because the gateway records locally.
 */
@Path("/")
@Produces(MediaType.TEXT_HTML)
public class OpsPages {
    private static final Logger LOG = Logger.getLogger(OpsPages.class);
    private static final Pattern ORDER_ID = Pattern.compile("^ord_[A-Za-z0-9_-]+$");
    private static final Pattern HOLD_ID = Pattern.compile("^hold_[a-f0-9]{32}$");
    private static final Set<String> REASONS = Set.of("DAMAGED", "QUALITY_CHECK", "COUNT_DISCREPANCY", "OTHER");

    private final GatewayClient gateway;

    public OpsPages(@RestClient GatewayClient gateway) {
        this.gateway = gateway;
    }

    @CheckedTemplate
    static class Templates {
        static native TemplateInstance index(String siteId, String instance);

        static native TemplateInstance picks(List<GatewayClient.PickItem> open, List<GatewayClient.PickItem> picked,
                Notice notice);

        static native TemplateInstance counts(List<GatewayClient.ActionResult> recent, Notice notice);

        static native TemplateInstance holds(List<GatewayClient.Hold> placed, Set<String> reasons, Notice notice);

        static native TemplateInstance buffer(Map<String, Object> buffer, boolean available);
    }

    @GET
    public TemplateInstance index() {
        try {
            Map<String, Object> info = gateway.info();
            return Templates.index(String.valueOf(info.get("siteId")), String.valueOf(info.get("instance")));
        } catch (WebApplicationException | ProcessingException error) {
            return Templates.index("unknown", "gateway unavailable");
        }
    }

    @GET
    @Path("/picks")
    public TemplateInstance picks() {
        return picks(null);
    }

    @POST
    @Path("/picks/{orderId}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public TemplateInstance pick(@PathParam("orderId") String orderId, @FormParam("requestId") String requestId) {
        if (!ORDER_ID.matcher(orderId).matches()) {
            return picks(Notice.error("Unknown order"));
        }
        return picks(act(() -> gateway.pick(Map.of("requestId", uuid(requestId), "orderId", orderId)),
                result -> "Picked " + result.quantity() + " × " + result.sku() + " for " + result.orderId()));
    }

    @GET
    @Path("/counts")
    public TemplateInstance counts() {
        return counts(null);
    }

    @POST
    @Path("/counts")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public TemplateInstance count(@FormParam("requestId") String requestId, @FormParam("sku") String sku,
            @FormParam("countedQuantity") String countedQuantity) {
        Integer quantity = integer(countedQuantity);
        if (quantity == null || sku == null || sku.isBlank()) {
            return counts(Notice.error("Enter a SKU and a whole number"));
        }
        return counts(act(() -> gateway.count(Map.of("requestId", uuid(requestId), "sku", sku.strip(),
                "countedQuantity", quantity)), result -> "Counted " + result.quantity() + " × " + result.sku()));
    }

    @GET
    @Path("/holds")
    public TemplateInstance holds() {
        return holds(null);
    }

    @POST
    @Path("/holds")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public TemplateInstance hold(@FormParam("requestId") String requestId, @FormParam("sku") String sku,
            @FormParam("quantity") String quantity, @FormParam("reasonCode") String reasonCode) {
        Integer amount = integer(quantity);
        if (amount == null || sku == null || sku.isBlank() || !REASONS.contains(reasonCode)) {
            return holds(Notice.error("Enter a SKU, a quantity and a reason"));
        }
        return holds(act(() -> gateway.hold(Map.of("requestId", uuid(requestId), "sku", sku.strip(), "quantity", amount,
                "reasonCode", reasonCode)), result -> "Hold placed on " + result.quantity() + " × " + result.sku()));
    }

    @POST
    @Path("/holds/{holdId}/release")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public TemplateInstance release(@PathParam("holdId") String holdId, @FormParam("requestId") String requestId) {
        if (!HOLD_ID.matcher(holdId).matches()) {
            return holds(Notice.error("Unknown hold"));
        }
        return holds(act(() -> gateway.release(holdId, Map.of("requestId", uuid(requestId))),
                result -> "Hold released on " + result.sku()));
    }

    @GET
    @Path("/buffer")
    public TemplateInstance buffer() {
        try {
            return Templates.buffer(gateway.buffer(), true);
        } catch (WebApplicationException | ProcessingException error) {
            return Templates.buffer(Map.of(), false);
        }
    }

    private TemplateInstance picks(Notice notice) {
        try {
            return Templates.picks(gateway.pickList("OPEN", 200), gateway.pickList("PICKED", 50), notice);
        } catch (WebApplicationException | ProcessingException error) {
            return Templates.picks(List.of(), List.of(), notice == null ? Notice.unavailable() : notice);
        }
    }

    private TemplateInstance counts(Notice notice) {
        try {
            return Templates.counts(gateway.counts(10), notice);
        } catch (WebApplicationException | ProcessingException error) {
            return Templates.counts(List.of(), notice == null ? Notice.unavailable() : notice);
        }
    }

    private TemplateInstance holds(Notice notice) {
        try {
            return Templates.holds(gateway.holds("PLACED"), REASONS, notice);
        } catch (WebApplicationException | ProcessingException error) {
            return Templates.holds(List.of(), REASONS, notice == null ? Notice.unavailable() : notice);
        }
    }

    private static Notice act(Action action, Describe describe) {
        try {
            GatewayClient.ActionResult result = action.run();
            return Notice.ok(describe.text(result) + (result.replayed() ? " (already recorded)" : ""));
        } catch (WebApplicationException error) {
            int status = error.getResponse().getStatus();
            return switch (status) {
                case 503 -> Notice.error("The site buffer is full: nothing was recorded. Try again when the site is back online.");
                case 409 -> Notice.error("Already done or no longer open. The list has been refreshed.");
                case 404 -> Notice.error("Not found. The list has been refreshed.");
                case 400 -> Notice.error("Check the values and try again.");
                default -> Notice.error("The gateway refused the action (HTTP " + status + ").");
            };
        } catch (ProcessingException error) {
            LOG.warnf("Gateway unreachable: %s", error.getMessage());
            return Notice.unavailable();
        }
    }

    private static String uuid(String value) {
        try {
            return UUID.fromString(value).toString();
        } catch (IllegalArgumentException | NullPointerException error) {
            throw new WebApplicationException(400);
        }
    }

    private static Integer integer(String value) {
        try {
            return value == null ? null : Integer.valueOf(value.strip());
        } catch (NumberFormatException error) {
            return null;
        }
    }

    /** Generates the requestId a form carries. */
    public static String newRequestId() {
        return UUID.randomUUID().toString();
    }

    /** A message shown above a list. */
    public record Notice(boolean ok, String text) {
        static Notice ok(String text) {
            return new Notice(true, text);
        }

        static Notice error(String text) {
            return new Notice(false, text);
        }

        static Notice unavailable() {
            return error("The gateway is not responding. Nothing was recorded.");
        }
    }

    @FunctionalInterface
    private interface Action {
        GatewayClient.ActionResult run();
    }

    @FunctionalInterface
    private interface Describe {
        String text(GatewayClient.ActionResult result);
    }
}
