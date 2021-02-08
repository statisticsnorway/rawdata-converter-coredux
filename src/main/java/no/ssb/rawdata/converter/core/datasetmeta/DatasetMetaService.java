package no.ssb.rawdata.converter.core.datasetmeta;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.micronaut.context.annotation.Context;
import io.micronaut.runtime.event.annotation.EventListener;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.dataset.api.DatasetId;
import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dapla.dataset.api.DatasetState;
import no.ssb.dapla.dataset.api.PseudoConfig;
import no.ssb.dapla.dataset.api.Type;
import no.ssb.dapla.dataset.api.Valuation;
import no.ssb.dapla.dataset.api.VarPseudoConfigItem;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.storage.client.backend.BinaryBackend;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.rawdata.converter.core.storage.BinaryBackendFactory;
import no.ssb.rawdata.converter.core.storage.StorageType;
import no.ssb.rawdata.converter.service.dapla.dataaccess.DataAccessService;
import no.ssb.rawdata.converter.service.dapla.dataaccess.ValidatedDatasetMeta;
import no.ssb.rawdata.converter.service.dapla.metadatadistributor.MetadataDistributorService;
import no.ssb.rawdata.converter.util.DatasetUriBuilder;

import javax.inject.Singleton;
import java.util.stream.Collectors;

@Singleton
@Context
@RequiredArgsConstructor
@Slf4j
public class DatasetMetaService {

    @NonNull
    private final BinaryBackendFactory binaryBackendFactory;

    @NonNull
    private final DataAccessService dataAccessService;

    @NonNull
    private final MetadataDistributorService metadataDistributorService;

    @EventListener
    void onPublishDatasetMeta(PublishDatasetMetaEvent e) {
        log.info("Receive PublishDatasetMetaEvent {}", e);
        DatasetMeta datasetMeta = datasetMetaOf(e);
        DatasetUri datasetUri = datasetUriOf(e);
        publishMetadata(datasetMeta, datasetUri);
    }

    /**
     * publishes metadata about a given dataset to make the dataset generally discoverable.
     *
     * @throws DatasetMetaPublishException if we fail to publish the metadata.
     */
    private void publishMetadata(DatasetMeta meta, DatasetUri datasetUri) throws DatasetMetaPublishException {
        log.info("Publish dataset meta for {}", datasetUri.toString());

        String metaJson = datasetMetaJsonOf(meta);

        // Get valid metadata, a signature and perform access check
        ValidatedDatasetMeta validMeta = dataAccessService.validateDatasetMeta(metaJson);

        // Create and upload metadata file
        String validMetaPath = datasetUri.toString() + "/.dataset-meta.json";
        String signaturePath = datasetUri.toString() + "/.dataset-meta.json.sign";

        // Store the metadata files to GCS
        storeDatasetMetaFiles(validMeta, validMetaPath, signaturePath);

        // Notify the metadata distributor about the creation of the metadata and signature files
        // TODO: Is this correct? ..or do we only need to publish the signed file?
        metadataDistributorService.publishFile(validMetaPath);
        metadataDistributorService.publishFile(signaturePath);

        log.info("Published metadata:\n{}", new String(validMeta.getContent()));
    }

    private void storeDatasetMetaFiles(ValidatedDatasetMeta datasetMeta, String metadataPath, String signaturePath) {
        // Assumes GCS with compute engine credentials
        BinaryBackend fileStorage = binaryBackendFactory.binaryBackendOf(StorageType.of(metadataPath));

        try {
            fileStorage.write(metadataPath, datasetMeta.getContent());
        } catch (Exception e) {
            throw new DatasetMetaPublishException(String.format("Failed to write %s", metadataPath), e);
        }

        try {
            fileStorage.write(signaturePath, datasetMeta.getSignature());
        } catch (Exception e) {
            throw new DatasetMetaPublishException(String.format("Failed to write %s", signaturePath), e);
        }
    }

    static String datasetMetaJsonOf(DatasetMeta meta) {
        try {
            return JsonFormat.printer().includingDefaultValueFields().print(meta);
        } catch (InvalidProtocolBufferException e) {
            throw new DatasetMetaPublishException("Could not convert DatasetMeta to JSON", e);
        }
    }

    static DatasetMeta datasetMetaOf(PublishDatasetMetaEvent e) {
        return DatasetMeta.newBuilder()
          .setId(
            DatasetId.newBuilder()
              .setPath(e.getStoragePath())
              .setVersion(e.getStorageVersion())
              .build()
          )
          .setState(DatasetState.RAW)
          .setValuation(Valuation.valueOf(e.getValuation().name()))
          .setType(Type.valueOf(e.getType().name()))
          .setCreatedBy("rawdata-converter") // TODO: get name and version from build
          .setPseudoConfig(
            PseudoConfig.newBuilder()
              .addAllVars(
                e.getPseudoRules().stream()
                  .map(r -> VarPseudoConfigItem.newBuilder()
                        .setVar(r.getPattern())
                        .setPseudoFunc(r.getFunc())
                        .build()
                  )
                  .collect(Collectors.toList()))
          )
          .build();
    }

    static DatasetUri datasetUriOf(PublishDatasetMetaEvent e) {
        return DatasetUriBuilder.of()
          .root(e.getStorageRoot())
          .path(e.getStoragePath())
          .version(e.getStorageVersion())
          .build();
    }

    public static class DatasetMetaPublishException extends RawdataConverterException {
        public DatasetMetaPublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
