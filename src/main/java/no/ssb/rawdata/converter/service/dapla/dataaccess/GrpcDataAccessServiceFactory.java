package no.ssb.rawdata.converter.service.dapla.dataaccess;

import io.grpc.ManagedChannelBuilder;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc.DataAccessServiceBlockingStub;
import no.ssb.rawdata.converter.service.dapla.oauth.AuthTokenProvider;

import javax.inject.Singleton;

@Factory
@RequiredArgsConstructor
@Slf4j
public class GrpcDataAccessServiceFactory {

    private final DataAccessServiceConfig dataAccessServiceConfig;

    @NonNull
    private final AuthTokenProvider authTokenProvider;

    @Singleton
    @Requires(property = DataAccessServiceConfig.PREFIX + ".impl", value = "GRPC")
    public DataAccessService grpcDataAccessService() {
        DataAccessServiceBlockingStub stub = DataAccessServiceGrpc.newBlockingStub(
          ManagedChannelBuilder
            .forAddress(
              dataAccessServiceConfig.getHost(),
              dataAccessServiceConfig.getPort()
            )
            .usePlaintext()
            .build()
        );

        return new GrpcDataAccessService(stub, authTokenProvider);
    }

}