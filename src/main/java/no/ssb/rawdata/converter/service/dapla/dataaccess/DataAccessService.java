package no.ssb.rawdata.converter.service.dapla.dataaccess;

import io.grpc.CallCredentials;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import no.ssb.dapla.data.access.protobuf.DataAccessServiceGrpc;
import no.ssb.dapla.data.access.protobuf.WriteLocationRequest;
import no.ssb.dapla.data.access.protobuf.WriteLocationResponse;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;

import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;

@Singleton
@RequiredArgsConstructor
public class DataAccessService {

    @NonNull
    private final DataAccessServiceGrpc.DataAccessServiceBlockingStub dataAccess;

    public ValidatedDatasetMeta validateDatasetMeta(String datasetMetaJson, CallCredentials credentials) {
        WriteLocationResponse writeLocationResponse;
        try {
            writeLocationResponse = dataAccess
              .withCallCredentials(credentials)
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

    public static class DataAccessServiceException extends RawdataConverterException {
        public DataAccessServiceException(String message, Throwable cause) {
            super(message, cause);
        }

        public DataAccessServiceException(String message) {
            super(message);
        }
    }

}
