package no.ssb.rawdata.converter.service.dapla.dataaccess;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class HttpDataAccessService implements DataAccessService {

    @NonNull
    private final DataAccessServiceConfig config;

    private final DataAccessHttpClient client;

    @Override
    public ValidatedDatasetMeta validateDatasetMeta(String datasetMetaJson) {
        DataAccessHttpClient.WriteLocationResponse res = client.writeLocation(
          DataAccessHttpClient.WriteLocationRequest.builder()
            .metadataJson(datasetMetaJson)
            .build())
          .blockingGet(); // TODO: Don't block
        return new ValidatedDatasetMeta(res.getValidMetadataJson().getBytes(), res.getMetadataSignature().getBytes());
    }

}
