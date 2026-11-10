package org.box1.gateway.agent;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Client-credentials access token of the site's Keycloak client (box1-docs#131). The secret is read from the file
 * cloud-init wrote (never from the image or the environment) on each token request, so a rotated secret applies
 * without a restart. The token is cached until 30 seconds before it expires. Nothing secret is logged.
 */
public class TokenSource {
    private final HttpClient http;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final URI tokenUrl;
    private final String clientId;
    private final Path secretFile;
    private String token;
    private Instant expiresAt = Instant.MIN;

    public TokenSource(HttpClient http, ObjectMapper mapper, Clock clock, URI tokenUrl, String clientId,
            Path secretFile) {
        this.http = http;
        this.mapper = mapper;
        this.clock = clock;
        this.tokenUrl = tokenUrl;
        this.clientId = clientId;
        this.secretFile = secretFile;
    }

    public synchronized String token() throws IOException, InterruptedException {
        if (token != null && clock.instant().isBefore(expiresAt)) {
            return token;
        }
        String secret = Files.readString(secretFile, StandardCharsets.UTF_8).strip();
        if (secret.isEmpty()) {
            throw new IOException("The client secret file is empty");
        }
        String form = "grant_type=client_credentials&client_id=" + encode(clientId) + "&client_secret=" + encode(secret);
        HttpRequest request = HttpRequest.newBuilder(tokenUrl).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/x-www-form-urlencoded").header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(form)).build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("Token request failed with HTTP " + response.statusCode());
        }
        JsonNode body = mapper.readTree(response.body());
        String accessToken = body.path("access_token").asText("");
        long expiresIn = body.path("expires_in").asLong(60);
        if (accessToken.isEmpty()) {
            throw new IOException("Token response has no access_token");
        }
        token = accessToken;
        expiresAt = clock.instant().plusSeconds(Math.max(0, expiresIn - 30));
        return token;
    }

    /** Forgets the cached token, for example after asset-service answered 401. */
    public synchronized void invalidate() {
        token = null;
        expiresAt = Instant.MIN;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
