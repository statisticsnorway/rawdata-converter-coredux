package no.ssb.rawdata.converter.service.dapla.dataaccess;

import io.micronaut.context.annotation.Requires;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

@Slf4j
@Singleton
@Requires(property = DataAccessServiceConfig.PREFIX + ".impl", value = "MOCK")
public class MockDataAccessService implements DataAccessService {

    @Override
    public ValidatedDatasetMeta validateDatasetMeta(String datasetMetaJson) {
        log.warn("DataAccessService MOCK - validateDatasetMeta");
        return new ValidatedDatasetMeta(datasetMetaJson.getBytes(), "mock".getBytes());
    }

}
