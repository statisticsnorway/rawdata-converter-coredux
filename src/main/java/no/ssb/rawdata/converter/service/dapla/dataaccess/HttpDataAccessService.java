package no.ssb.rawdata.converter.service.dapla.dataaccess;

import io.micronaut.context.annotation.Requires;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.rawdata.converter.service.dapla.metadatadistributor.MetadataDistributorServiceConfig;

import javax.inject.Singleton;

@Slf4j
@RequiredArgsConstructor
@Singleton
@Requires(property = DataAccessServiceConfig.PREFIX + ".impl", value = "HTTP")
public class HttpDataAccessService implements DataAccessService {

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
