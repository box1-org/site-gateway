package org.box1.gateway.contract;

import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

/**
 * Validates produced events and the heartbeat body against box1-ops-contracts at the pinned commit (contracts.ref),
 * fetched from GitHub. No local copy of a shared schema.
 */
public final class ContractSchemas {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final SchemaValidatorsConfig CONFIG = SchemaValidatorsConfig.builder().formatAssertionsEnabled(true)
            .build();

    private ContractSchemas() {
    }

    public static void assertValidEvent(Map<String, Object> event) throws Exception {
        JsonNode node = JSON.valueToTree(event);
        String type = node.path("eventType").asText().replaceAll("\\.v[0-9]+$", "");
        check("envelope", node, schema("events/v1/envelope.schema.json"));
        check(type, node.path("payload"), schema("events/v1/" + type + ".schema.json"));
    }

    /** Validates against a schema in openapi/asset-service.yaml components, with its $refs resolved locally. */
    public static void assertValidAssetSchema(String component, Map<String, Object> body) throws Exception {
        JsonNode openapi;
        try (InputStream in = url("openapi/asset-service.yaml").openStream()) {
            openapi = new ObjectMapper(new YAMLFactory()).readTree(in);
        }
        ObjectNode defs = (ObjectNode) openapi.path("components").path("schemas").deepCopy();
        rewriteRefs(defs);
        ObjectNode root = ((ObjectNode) defs.get(component)).deepCopy();
        root.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        root.set("$defs", defs);
        JsonSchema schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(root, CONFIG);
        check(component, JSON.valueToTree(body), schema);
    }

    private static void rewriteRefs(JsonNode node) {
        if (node instanceof ObjectNode object) {
            JsonNode ref = object.get("$ref");
            if (ref != null && ref.asText().startsWith("#/components/schemas/")) {
                object.set("$ref", TextNode.valueOf(ref.asText().replace("#/components/schemas/", "#/$defs/")));
            }
            for (Iterator<JsonNode> it = object.elements(); it.hasNext();) {
                rewriteRefs(it.next());
            }
        } else if (node != null && node.isArray()) {
            node.forEach(ContractSchemas::rewriteRefs);
        }
    }

    private static JsonSchema schema(String path) throws Exception {
        try (InputStream in = url(path).openStream()) {
            return JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012).getSchema(in, CONFIG);
        }
    }

    private static java.net.URL url(String path) throws Exception {
        String ref = System.getProperty("contracts.ref");
        if (ref == null || ref.isBlank()) {
            throw new AssertionError("contracts.ref is not set");
        }
        return URI.create("https://raw.githubusercontent.com/box1-org/box1-ops-contracts/" + ref + "/" + path).toURL();
    }

    private static void check(String name, JsonNode node, JsonSchema schema) {
        Set<ValidationMessage> errors = schema.validate(node);
        if (!errors.isEmpty()) {
            throw new AssertionError(name + " invalid: " + errors);
        }
    }
}
