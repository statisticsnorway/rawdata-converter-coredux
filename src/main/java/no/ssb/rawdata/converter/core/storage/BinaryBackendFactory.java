package no.ssb.rawdata.converter.core.storage;

import com.google.common.base.Strings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.storage.client.backend.BinaryBackend;
import no.ssb.dapla.storage.client.backend.gcs.GoogleCloudStorageBackend;
import no.ssb.dapla.storage.client.backend.local.LocalBackend;

import javax.inject.Singleton;
import java.nio.file.Files;
import java.nio.file.Path;

@RequiredArgsConstructor
@Slf4j
@Singleton
public class BinaryBackendFactory {

    public BinaryBackend binaryBackendOf(StorageType storageType) {
        return binaryBackendOf(storageType, null);
    }

    // TODO: Rename to create
    public BinaryBackend binaryBackendOf(StorageType storageType, String saKeyFile) {

        switch (storageType) {
            case FILESYSTEM:
                log.info("Use local filesystem data storage client");
                return new LocalBackend();
            case GCS:
                GoogleCloudStorageBackend.Configuration configuration = new GoogleCloudStorageBackend.Configuration().setReadChunkSize(4 * 1024 * 1024).setWriteChunkSize(4 * 1024 * 1024);
                if (Strings.isNullOrEmpty(saKeyFile)) {
                    log.info("Use GCS data storage client with compute engine credentials");
                } else {
                    Path saKeyFilePath = Path.of(saKeyFile);
                    if (Files.notExists(saKeyFilePath)) {
                        throw new IllegalArgumentException("Could not find service account file " + saKeyFile);
                    }
                    configuration.setServiceAccountCredentials(saKeyFilePath);
                    log.info("Use GCS data storage client with service account key file '{}'", saKeyFilePath.toString());
                }
                return new GoogleCloudStorageBackend(configuration);
            default:
                throw new IllegalStateException("Unsupported data storage type: " + storageType);
        }
    }

}
