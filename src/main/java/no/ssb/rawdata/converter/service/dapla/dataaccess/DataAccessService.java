package no.ssb.rawdata.converter.service.dapla.dataaccess;

import no.ssb.rawdata.converter.core.exception.RawdataConverterException;

public interface DataAccessService {

    ValidatedDatasetMeta validateDatasetMeta(String datasetMetaJson);

    class DataAccessServiceException extends RawdataConverterException {
        public DataAccessServiceException(String message, Throwable cause) {
            super(message, cause);
        }

        public DataAccessServiceException(String message) {
            super(message);
        }
    }

}
