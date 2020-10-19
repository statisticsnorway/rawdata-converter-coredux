package no.ssb.rawdata.converter.core.security;

import io.grpc.CallCredentials;
import io.grpc.Metadata;

import java.util.concurrent.Executor;

// TODO: Move elsewhere

/**
 * GrpcAuthorizationBearerCallCredentials enables adding an HTTP/2 authorization header with bearer token to a grpc
 * call.
 * Usage:
 * <pre>
 * GrpcAuthorizationBearerCallCredentials credentials = GrpcAuthorizationBearerCallCredentials.create("token");
 * response = FooGrpc.newStub(channel).withCallCredentials(credentials).bar(request);
 * </pre>
 */
public class GrpcAuthorizationBearerCallCredentials extends CallCredentials {

    private final String token;

    private GrpcAuthorizationBearerCallCredentials(String token) {
        this.token = token;
    }

    public static GrpcAuthorizationBearerCallCredentials create(String token) {
        return new GrpcAuthorizationBearerCallCredentials(token);
    }

    @Override
    public void applyRequestMetadata(RequestInfo requestInfo, Executor executor, MetadataApplier metadataApplier) {
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER), String.format("Bearer %s", this.token));
        executor.execute(() -> {
            metadataApplier.apply(metadata);
        });
    }

    @Override
    public void thisUsesUnstableApi() {

    }
}
