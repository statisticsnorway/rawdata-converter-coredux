package no.ssb.rawdata.converter.service.dapla.oauth;

import com.fasterxml.jackson.core.type.TypeReference;
import no.ssb.rawdata.converter.util.Json;

import javax.inject.Singleton;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static java.time.temporal.ChronoUnit.SECONDS;

@Singleton
public class KeycloakAuthTokenProvider implements AuthTokenProvider {

    private final HttpClient httpClient;
    private final OauthServiceConfig config;

    public KeycloakAuthTokenProvider(OauthServiceConfig config) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.of(10, SECONDS))
                .build();
        this.config = config;
    }

    @Override
    public String getAuthToken() {

        HttpRequest request = HttpRequest.newBuilder()
                .uri(config.getTokenUrl())
                .POST(BodyPublishers.ofString("grant_type=client_credentials"))
                .timeout(Duration.of(10, SECONDS))
                .header("Content-Type", MediaType.APPLICATION_FORM_URLENCODED)
                .header("Authorization", String.format("Basic %s", config.getBase64Credentials()))
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            throw new AuthTokenRetrievalException(String.format("Unable to retrieve auth token from Keycloak server %s", config.getTokenUrl()), e);
        }

        if (200 != response.statusCode()) {
            throw new AuthTokenRetrievalException(String.format("Got non 200 OK response. code: %d - body:\n%s", response.statusCode(), response.body()));
        }

        Map<String, String> responseBody;
        try {
            responseBody = Json.toObject(new TypeReference<HashMap<String, String>>() {}, response.body());
        } catch (Exception e) {
            throw new AuthTokenRetrievalException(String.format("Could not parse response body %s", response.body()), e);
        }

        String token = responseBody.get("access_token");
        if (null == token) {
            throw new AuthTokenRetrievalException(String.format("Response did not contain 'access_token' %s", response.body()));
        }

        return token;
    }

    public static class AuthTokenRetrievalException extends RuntimeException {
        public AuthTokenRetrievalException(String message) {
            super(message);
        }

        public AuthTokenRetrievalException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
