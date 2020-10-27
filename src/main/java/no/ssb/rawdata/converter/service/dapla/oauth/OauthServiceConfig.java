package no.ssb.rawdata.converter.service.dapla.oauth;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.Base64;

@ConfigurationProperties("services.dapla-oauth")
@Data
@Slf4j
public class OauthServiceConfig {

    private String host;

    private String tokenEndpointPath;

    private String clientId;

    private String clientSecret;

    public String getBase64Credentials() {
        if (clientId == null || clientSecret == null) {
            log.warn("services.dapla-oauth.client-id and services.dapla-oauth.client-secret are not specified. Will not be able to obtain auth token to invoke other services.");
        }
        return Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes());
    }

    // TODO: Make this joining more robust
    public URI getTokenUrl() {
        return URI.create(host + tokenEndpointPath);
    }

}
