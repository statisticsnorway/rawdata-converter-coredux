package no.ssb.rawdata.converter.service.dapla.metadatadistributor;

import io.grpc.CallCredentials;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc;
import no.ssb.rawdata.converter.core.security.GrpcAuthorizationBearerCallCredentials;
import no.ssb.rawdata.converter.service.dapla.oauth.AuthTokenProvider;

import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class GrpcMetadataDistributorService implements MetadataDistributorService {

    @NonNull private final MetadataDistributorServiceConfig config;
    @NonNull private final MetadataDistributorServiceGrpc.MetadataDistributorServiceBlockingStub metadataDistributor;
    @NonNull private final AuthTokenProvider authTokenProvider;

    /**
     * Notify the metadata distributor about the creation of a metadata file.
     */
    public void publishFile(String fileUri) {
        try {
            metadataDistributor
              .withCallCredentials(getGrpcCallCredentials())
              .withDeadlineAfter(10, TimeUnit.SECONDS)
              .dataChanged(
                DataChangedRequest.newBuilder()
                  .setProjectId(config.getProjectId())
                  .setTopicName(config.getTopic())
                  .setUri(fileUri)
                  .build()
              );
        } catch (Exception e) {
            throw new MetadataDistributorServiceException(String.format("Failed to notify metadata distributor about file. URI: %s - Topic: %s - ProjectId: %s", fileUri, config.getTopic(), config.getProjectId()), e);
        }
    }

    CallCredentials getGrpcCallCredentials() {
        String authToken = authTokenProvider.getAuthToken();
        GrpcAuthorizationBearerCallCredentials callCredentials = GrpcAuthorizationBearerCallCredentials.create(authToken);
        return callCredentials;
    }

}
