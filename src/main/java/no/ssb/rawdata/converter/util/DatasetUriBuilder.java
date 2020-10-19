package no.ssb.rawdata.converter.util;

import no.ssb.dapla.dataset.uri.DatasetUri;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Create a DatasetUri for the given root, path and version fragments
 *
 * Takes care of relative to absolute file paths, if applicable.
 *
 */
public class DatasetUriBuilder {

    private String root;
    private String path;
    private String version;

    public static DatasetUriBuilder of() {
        return new DatasetUriBuilder();
    }

    /**
     * @param root the uri root, typically prefixed by scheme
     */
    public DatasetUriBuilder root(String root) {
        this.root = root;
        if (root.startsWith("file://.")) {
            String relativePath = root.substring(7);
            try {
                this.root = "file://" + Path.of(relativePath).toRealPath().toString();
            }
            catch (IOException e) {
                throw new IllegalArgumentException("Unable to resolve file path from " + root);
            }
        }

        return this;
    }

    /**
     * @param path the path to the dataset relative to the root
     */
    public DatasetUriBuilder path(String path) {
        this.path = path;
        return this;
    }

    /**
     * @param version dataset version, typically a timestamp
     */
    public DatasetUriBuilder version(String version) {
        this.version = version;
        return this;
    }

    public DatasetUri build() {
        Objects.requireNonNull(root, "Missing root fragment");
        return DatasetUri.of(root, path, version);
    }

}
