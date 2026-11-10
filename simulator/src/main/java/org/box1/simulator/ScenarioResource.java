package org.box1.simulator;

import java.util.Map;

import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/** Local control of the simulator (the simulator listens on 127.0.0.1 only). */
@Path("/api/v1/simulator")
@Produces(MediaType.APPLICATION_JSON)
public class ScenarioResource {
    private final Simulator simulator;

    public ScenarioResource(Simulator simulator) {
        this.simulator = simulator;
    }

    @GET
    public Map<String, Object> status() {
        return simulator.status();
    }

    /** Sends each asset's breach value for {@code ticks} readings (1..60), then returns to normal values. */
    @POST
    @Path("/breach")
    @Consumes(MediaType.APPLICATION_JSON)
    public Map<String, Object> breach(BreachRequest request) {
        if (request == null || request.ticks() == null || request.ticks() < 1 || request.ticks() > 60) {
            throw new BadRequestException("ticks must be 1..60");
        }
        simulator.breach(request.ticks());
        return simulator.status();
    }

    /** Breach request. */
    public record BreachRequest(Integer ticks) {
    }
}
