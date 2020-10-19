package no.ssb.rawdata.converter.service.dapla.dataaccess;

import io.grpc.ManagedChannelBuilder;
import io.micronaut.context.annotation.Factory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc.DataAccessServiceBlockingStub;

import javax.inject.Singleton;


@Factory
@RequiredArgsConstructor
@Slf4j
public class DataAccessServiceFactory {

    private final DataAccessServiceConfig dataAccessServiceConfig;

    /**
     * Grpc *blocking* stub of the data access service.
     */
    @Singleton
    public DataAccessServiceBlockingStub dataAccessService() {
        return DataAccessServiceGrpc.newBlockingStub(
                ManagedChannelBuilder
                        .forAddress(
                          dataAccessServiceConfig.getHost(),
                          dataAccessServiceConfig.getPort()
                        )
                        .usePlaintext()
                        .build()
        );
    }
}
