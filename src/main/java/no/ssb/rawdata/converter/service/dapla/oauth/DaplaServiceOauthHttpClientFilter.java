package no.ssb.rawdata.converter.service.dapla.oauth;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.annotation.Filter;
import io.micronaut.http.filter.ClientFilterChain;
import io.micronaut.http.filter.HttpClientFilter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.reactivestreams.Publisher;

/**
 * HttpClientFilter that decorates all outgoing requests to dapla services (such as metadata distributor and the data access service)
 * with an Authorization Bearer token
 */
@Filter(patterns = {
  "/rpc/**"
})
@Requires(beans = KeycloakAuthTokenProvider.class )
@RequiredArgsConstructor
public class DaplaServiceOauthHttpClientFilter implements HttpClientFilter {

    @NonNull
    private final AuthTokenProvider authTokenProvider;

    @Override
    public Publisher<? extends HttpResponse<?>> doFilter(MutableHttpRequest<?> request, ClientFilterChain chain) {
        return chain.proceed((request.bearerAuth(authTokenProvider.getAuthToken())));
    }

}

