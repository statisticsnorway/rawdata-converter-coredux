package no.ssb.rawdata.converter.service.dapla.dataaccess;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockDataAccessService implements DataAccessService {

    @Override
    public ValidatedDatasetMeta validateDatasetMeta(String datasetMetaJson) {
        log.warn("DataAccessService MOCK - validateDatasetMeta");
        return new ValidatedDatasetMeta(datasetMetaJson.getBytes(), "mock".getBytes());
    }

}
