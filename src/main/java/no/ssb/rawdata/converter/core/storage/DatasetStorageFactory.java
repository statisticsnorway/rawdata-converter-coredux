package no.ssb.rawdata.converter.core.storage;

import io.micronaut.context.annotation.Factory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.storage.client.DatasetStorage;
import no.ssb.dapla.storage.client.ParquetProvider;
import no.ssb.dapla.storage.client.WriteExceptionHandler;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

@Factory
@RequiredArgsConstructor
@Slf4j
public class DatasetStorageFactory {

    private final BinaryBackendFactory binaryBackendFactory;

    public DatasetStorage datasetStorageOf(StorageType storageType, String saKeyFile) {
        int parquetRowGroupSize = 64 * 1024 * 1024;
        int parquetPageSize = 8 * 1024 * 1024;

        return DatasetStorage.builder()
          .withParquetProvider(new ParquetProvider(parquetRowGroupSize, parquetPageSize))
          .withBinaryBackend(binaryBackendFactory.binaryBackendOf(storageType, saKeyFile))
          //.withWriteExceptionHandler(new CustomWriteExceptionHandler())
          .build();
    }

    // TODO: Remove this?
    static class CustomWriteExceptionHandler implements WriteExceptionHandler {

        private static final Logger log = LoggerFactory.getLogger(CustomWriteExceptionHandler.class);

        @Override
        public Optional<GenericRecord> handleException(Exception e, GenericRecord record) {
            String exceptionMsg = Optional.ofNullable(e.getMessage()).orElse("");

            if (exceptionMsg.contains("Array contains a null element at")) {
                log.warn("Error writing record with array containing null elements:\n" + record, e);
            }
            else if (e instanceof RuntimeException) {
                log.warn("Error writing record:\n" + record, e);
            }

            return Optional.empty();
        }
    }

}
