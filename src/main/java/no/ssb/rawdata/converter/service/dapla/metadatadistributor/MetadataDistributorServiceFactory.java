package no.ssb.rawdata.converter.service.dapla.metadatadistributor;

import io.grpc.ManagedChannelBuilder;
import io.micronaut.context.annotation.Factory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc.MetadataDistributorServiceBlockingStub;

import javax.inject.Singleton;

@Factory
@RequiredArgsConstructor
@Slf4j
public class MetadataDistributorServiceFactory {

    private final MetadataDistributorServiceConfig metadataDistributorServiceConfig;

    /**
     * Grpc *blocking* stub of the data metadata distributor service.
     */
    @Singleton
    public MetadataDistributorServiceBlockingStub metadataDistributorService() {
        return MetadataDistributorServiceGrpc.newBlockingStub(
                ManagedChannelBuilder
                        .forAddress(
                          metadataDistributorServiceConfig.getHost(),
                          metadataDistributorServiceConfig.getPort()
                        )
                        .usePlaintext()
                        .build()
        );
    }
}
