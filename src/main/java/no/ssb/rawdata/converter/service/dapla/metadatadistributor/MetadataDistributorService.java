package no.ssb.rawdata.converter.service.dapla.metadatadistributor;

import io.grpc.CallCredentials;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import no.ssb.dapla.metadata.distributor.protobuf.DataChangedRequest;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;

import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;

@Singleton
@RequiredArgsConstructor
public class MetadataDistributorService {

    @NonNull private final MetadataDistributorServiceConfig config;
    @NonNull private final MetadataDistributorServiceGrpc.MetadataDistributorServiceBlockingStub metadataDistributor;

    /**
     * Notify the metadata distributor about the creation of a metadata file.
     */
    public void publishFile(String fileUri, CallCredentials credentials) {
        try {
            metadataDistributor
              .withCallCredentials(credentials)
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

    public static class MetadataDistributorServiceException extends RawdataConverterException {
        public MetadataDistributorServiceException(String message, Throwable cause) {
            super(message, cause);
        }

        public MetadataDistributorServiceException(String message) {
            super(message);
        }
    }

}
