package no.ssb.rawdata.converter.service.dapla.metadatadistributor;

import no.ssb.rawdata.converter.core.exception.RawdataConverterException;

public interface MetadataDistributorService {

    /**
     * Notify the metadata distributor about the creation of a metadata file.
     */
     void publishFile(String fileUri);

    class MetadataDistributorServiceException extends RawdataConverterException {
        public MetadataDistributorServiceException(String message, Throwable cause) {
            super(message, cause);
        }

        public MetadataDistributorServiceException(String message) {
            super(message);
        }
    }

}
