package no.ssb.rawdata.converter.service.dapla.dataaccess;

import com.sun.jersey.core.util.Base64;
import io.micronaut.context.annotation.Requires;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
          .blockingGet();

        return new ValidatedDatasetMeta(
          Base64.decode(res.getValidMetadataJson()),
          Base64.decode(res.getMetadataSignature())
        );
    }

}
