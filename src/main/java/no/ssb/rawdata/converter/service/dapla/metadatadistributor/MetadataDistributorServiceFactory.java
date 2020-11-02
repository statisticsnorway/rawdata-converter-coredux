package no.ssb.rawdata.converter.service.dapla.metadatadistributor;

import io.grpc.ManagedChannelBuilder;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc;
import no.ssb.dapla.metadata.distributor.protobuf.MetadataDistributorServiceGrpc.MetadataDistributorServiceBlockingStub;
import no.ssb.rawdata.converter.service.dapla.dataaccess.DataAccessService;
import no.ssb.rawdata.converter.service.dapla.dataaccess.MockDataAccessService;
import no.ssb.rawdata.converter.service.dapla.oauth.AuthTokenProvider;

import javax.inject.Singleton;

@Factory
@RequiredArgsConstructor
@Slf4j
public class MetadataDistributorServiceFactory {

    private final MetadataDistributorServiceConfig metadataDistributorServiceConfig;

    @NonNull
    private final AuthTokenProvider authTokenProvider;

    @Singleton
    @Requires(property = "services.dapla-metadata-distributor.impl", value = "GRPC")
    public MetadataDistributorService grpcMetadataDistributorService() {
        MetadataDistributorServiceBlockingStub stub = MetadataDistributorServiceGrpc.newBlockingStub(
          ManagedChannelBuilder
            .forAddress(
              metadataDistributorServiceConfig.getHost(),
              metadataDistributorServiceConfig.getPort()
            )
            .usePlaintext()
            .build()
        );

        return new GrpcMetadataDistributorService(metadataDistributorServiceConfig, stub, authTokenProvider);
    }

    @Singleton
    @Requires(property = "services.dapla-metadata-distributor.impl", value = "MOCK")
    public MetadataDistributorService mockMetadataDistributorService() {
        return new MockMetadataDistributorService();
    }

}
