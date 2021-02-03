package no.ssb.rawdata.converter.core.rawdatasource;

import de.huxhorn.sulky.ulid.ULID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.storage.client.DatasetStorage;
import no.ssb.dapla.storage.client.backend.FileInfo;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.rawdata.converter.core.job.ConverterJobConfig;
import no.ssb.rawdata.converter.core.storage.DatasetStorageFactory;
import no.ssb.rawdata.converter.core.storage.StorageType;
import no.ssb.rawdata.converter.core.storage.UlidVisitor;
import no.ssb.rawdata.converter.util.DatasetUriBuilder;

import javax.inject.Singleton;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

@Singleton
@Slf4j
@RequiredArgsConstructor
public class RawdataConsumerFactory {

    private final RawdataClientFactory rawdataClientFactory;
    private final DatasetStorageFactory datasetStorageFactory;

    public RawdataConsumers rawdataConsumersOf(ConverterJobConfig jobConfig) {
        DatasetUri datasetUri = datasetUriOf(jobConfig.getTargetStorage());
        StorageType storageType = StorageType.of(datasetUri);
        DatasetStorage datasetStorage = datasetStorageFactory.datasetStorageOf(storageType, jobConfig.getTargetStorage().getSaKeyFile());
        ULID.Value initialPosition = resolveInitialPosition(jobConfig.getRawdataSource().getInitialPosition(), datasetStorage, datasetUri);

        RawdataClient rawdataClient = rawdataClientFactory.rawdataClientOf(jobConfig.getRawdataSource().getName());
        RawdataConsumer mainRawdataConsumer = rawdataClient.consumer(jobConfig.getRawdataSource().getTopic(), initialPosition, true);
        RawdataConsumer sampleRawdataConsumer = rawdataClient.consumer(jobConfig.getRawdataSource().getTopic());

        return RawdataConsumers.builder()
          .mainRawdataConsumer(mainRawdataConsumer)
          .sampleRawdataConsumer(sampleRawdataConsumer)
          .build();
    }

    private static DatasetUri datasetUriOf(ConverterJobConfig.TargetStorage storage) {
        return DatasetUriBuilder.of()
          .root(storage.getRoot())
          .path(storage.getPath())
          .version(storage.getVersion())
          .build();
    }

    /**
     * Attempt to resolve the position from which the rawdata stream should start.
     */
    // TODO: Return Optional
    private ULID.Value resolveInitialPosition(String initialPosition, DatasetStorage datasetStorage, DatasetUri datasetUri) {
        final ULID.Value position;
        try {
            if ("FIRST".equalsIgnoreCase(initialPosition)) {
                position = null;
            } else if ("LAST".equalsIgnoreCase(initialPosition)) {
                log.info("Determine initial starting position by searching for last record in {}", datasetUri);
                position = attemptToFindLastRecord(datasetStorage, datasetUri);
            } else {
                position = ULID.parseULID(initialPosition);
            }

        } catch (LastPositionNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new RawdataConverterException("Unable to determine initial rawdata converter starting position. Make sure that the rawdata.converter.initial-position param is either 'LAST', 'FIRST' or a valid ULID. Was: '" + initialPosition + "'", e);
        }
        if (position == null) {
            log.info("Rawdata conversion will start from the beginning of the rawdata stream");
        } else {
            log.info("Start rawdata conversion at {}", keyValue("position", position.toString()));
        }
        return position;
    }

    // TODO: Return Optional
    private ULID.Value attemptToFindLastRecord(DatasetStorage datasetStorage, DatasetUri datasetUri) {
        FileInfo lastModifiedDatasetFile = datasetStorage.getLastModifiedDatasetFile(datasetUri).orElse(null);
        if (lastModifiedDatasetFile == null) {
            return null;
        }

        try {
            UlidVisitor ulidVisitor = new UlidVisitor();
            datasetStorage.readParquetFile(datasetUri, lastModifiedDatasetFile.getName(), UlidVisitor.ULID_PROJECTION_SCHEMA, ulidVisitor);
            return ulidVisitor.getLatest();
        }
        catch (Exception e) {
            throw new LastPositionNotFoundException(datasetUri, e);
        }
    }

    public static class LastPositionNotFoundException extends RawdataConverterException {
        public LastPositionNotFoundException(DatasetUri datasetUri, Exception e) {
            super("Unable to determine rawdata converter starting position. Error searching for LAST position of rawdata uri '" + datasetUri + "'", e);
        }
    }

}
