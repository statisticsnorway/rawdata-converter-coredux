package no.ssb.rawdata.converter.service.dapla.dataaccess;

import io.grpc.CallCredentials;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.rawdata.converter.core.security.GrpcAuthorizationBearerCallCredentials;
import no.ssb.rawdata.converter.service.dapla.oauth.AuthTokenProvider;

import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class GrpcDataAccessService implements DataAccessService{

    @NonNull
    private final DataAccessServiceGrpc.DataAccessServiceBlockingStub dataAccess;

    @NonNull
    private final AuthTokenProvider authTokenProvider;

    @Override
    public ValidatedDatasetMeta validateDatasetMeta(String datasetMetaJson) {
        WriteLocationResponse writeLocationResponse;
        try {
            writeLocationResponse = dataAccess
              .withCallCredentials(getGrpcCallCredentials())
              .withDeadlineAfter(10, TimeUnit.SECONDS)
              .writeLocation(
                WriteLocationRequest.newBuilder()
                  .setMetadataJson(datasetMetaJson)
                  .build()
              );
        } catch (Exception e) {
            throw new DataAccessServiceException(String.format("Failed to get valid metadata from data access service. input metadata:\n%s", datasetMetaJson), e);
        }

        //Check if we have access
        if (!writeLocationResponse.getAccessAllowed()) {
            throw new DataAccessServiceException("Got access not allowed from the dapla data access service");
        }

        return new ValidatedDatasetMeta(writeLocationResponse.getValidMetadataJson().toByteArray(), writeLocationResponse.getMetadataSignature().toByteArray());
    }

    CallCredentials getGrpcCallCredentials() {
        String authToken = authTokenProvider.getAuthToken();
        GrpcAuthorizationBearerCallCredentials callCredentials = GrpcAuthorizationBearerCallCredentials.create(authToken);
        return callCredentials;
    }

}
