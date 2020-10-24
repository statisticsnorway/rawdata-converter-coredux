package no.ssb.rawdata.converter.service.dapla.oauth;

import io.micronaut.context.annotation.Requires;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

@Singleton
@Requires(property = "services.dapla-oauth.token-provider", value = "mock")
@Slf4j
public class MockAuthTokenProvider implements AuthTokenProvider {

    public MockAuthTokenProvider() {
        log.warn("MockAuthTokenProvider is active. External service request will not be allowed. This should not be used in a production environment.");
    }

    @Override
    public String getAuthToken() {
        return "mock-token";
    }

}
