package no.ssb.rawdata.converter.core.storage;

import no.ssb.dapla.dataset.uri.DatasetUri;

public enum StorageType {
    FILESYSTEM, GCS;

    public static StorageType of(String rootUri) {
        if (rootUri.startsWith("gs://")) {
            return GCS;
        }
        else if (rootUri.startsWith("file://")) {
            return FILESYSTEM;
        }
        else {
            throw new IllegalArgumentException("Unknown storage type for '" + rootUri + "'");
        }
    }

    public static StorageType of(DatasetUri datasetUri) {
        return of(datasetUri.toString());
    }
}
