package no.ssb.rawdata.converter.core.job;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Introspected;
import lombok.Data;
import lombok.experimental.Accessors;
import no.ssb.dlp.pseudo.core.PseudoFuncRule;
import no.ssb.rawdata.converter.core.datasetmeta.DatasetType;
import no.ssb.rawdata.converter.core.datasetmeta.Valuation;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Context
@Introspected
@Data @Accessors(chain = true)
@EachProperty(value = "rawdata.converter.jobs")
public class ConverterJobConfig implements Serializable {

    public ConverterJobConfig(@Parameter String name) {
        this.name = name;
    }

    /**
     * The job name
     */
    private String name;

    /**
     * If true then this ConverterJobConfig is an abstract configuration ("prototype"). Prototype configs only serve
     * as configuration carriers that non-prototype configs can inherit common ground configuration from. They cannot
     * be scheduled for execution.
     */
    @NotInherited
    private Boolean prototype;

    /**
     * Optionally pointing at another configuration that this ConverterJobConfig will inherit properties from.
     * If not specified it will inherit directly from the "default" ConverterJobConfig (the grandma of all
     * ConverterJobConfigs)
     */
    private String parent;

    /**
     * If true, this job will be started automatically when the job has gone through the initialization stage.
     * For jobs specified in the application configuration, the job will be started immediately after the application
     * is ready.
     */
    private Boolean activeByDefault; // TODO: rename?

    /**
     * Runtime properties that supports the development and debugging process
     */
    private Debug debug = new Debug();

    /**
     * General converter settings
     */
    private ConverterSettings converterSettings = new ConverterSettings();

    /**
     * Rawdata source properties specific for the job
     */
    private RawdataSourceRef rawdataSource = new RawdataSourceRef();

    /**
     * Where the converted dataset is stored
     */
    private TargetStorage targetStorage = new TargetStorage();

    /**
     * Metadata about the converted dataset
     */
    private TargetDataset targetDataset = new TargetDataset();

    public boolean isPrototype() {
        return Optional.ofNullable(prototype).orElse(false);
    }

    public boolean isActiveByDefault() {
        return Optional.ofNullable(activeByDefault).orElse(false);
    }

    private List<PseudoFuncRule> pseudoRules = new ArrayList<>();

//    private Map<String, Object> converterConfig = new HashMap<>();
//    public void setConverterConfig(@MapFormat(transformation = MapFormat.MapTransformation.NESTED, keyFormat = StringConvention.CAMEL_CASE) Map<String, Object> converterConfig) {
//        this.converterConfig = converterConfig;
//    }

    @Introspected
    static abstract class ConfigElement implements Serializable {}

    /**
     * Annotation used to mark config properties that should not be inherited when computing effective ConverterJobConfigs
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface NotInherited {}

    @ConfigurationProperties("debug")
    @Data
    public static class Debug extends ConfigElement {
        /**
         * <p>If true, no data will be written. This can be handy during development, as it allows
         * you to run the converter in "simulation mode" against a stream of rawdata.</p>
         *
         * <p>Default: false.</p>
         */
        private Boolean dryrun;

        /**
         * <p>If true, the converter will be running in development mode. Certain warnings will
         * be suppressed, and logs might contain more (and in some cases sensitive) information
         * to help pinpoint issues.</p>
         *
         * <p>Needless to say, this SHOULD NOT be active in a production environment.</p>
         */
        private Boolean developmentMode;

        /**
         * <p>If true, all failed rawdata messages will be logged.</p>
         *
         * <p>Default: false</p>
         */
        private Boolean logFailedRawdata;

        /**
         * <p>If true, all skipped rawdata messages will be logged.</p>
         *
         * <p>Default: false</p>
         */
        private Boolean logSkippedRawdata;

        /**
         * <p>If true, all rawdata messages will be logged.</p>
         *
         * <p>Default: false</p>
         */
        private Boolean logAllRawdata;

        /**
         * <p>If true, all converted records will be logged.</p>
         *
         * <p>Default: false</p>
         */
        private Boolean logAllConverted;

        /**
         * <p>If true, all failed rawdata messages will be stored to
         * local disk (specified by localStoragePath).</p>
         *
         * <p>Default: false</p>
         */
        private Boolean storeFailedRawdata;

        /**
         * <p>If true, all skipped rawdata messages will be stored to
         * local disk (specified by localStoragePath).</p>
         *
         * <p>Default: false</p>
         */
        private Boolean storeSkippedRawdata;

        /**
         * <p>If true, all rawdata messages will be stored to
         * local disk (specified by localStoragePath).</p>
         *
         * <p>Default: false</p>
         */
        private Boolean storeAllRawdata;

        /**
         * <p>If true, all converted records will be stored as JSON to
         * local disk (specified by localStoragePath).</p>
         *
         * <p>Default: false</p>
         */
        private Boolean storeAllConverted;

        /**
         * <p>The root path of locally stored debug content.</p>
         *
         * <p>This must be specified if any of the following properties are true:
         * <ul>
         *     <li>storeFailedRawdata</li>
         *     <li>storeSkippedRawdata</li>
         *     <li>storeAllRawdata</li>
         *     <li>storeAllConverted</li>
         * </ul></p>
         */
        private String localStoragePath;

        /**
         * If specified, items will be added to a password protected archive. Can be used to add additional
         * security to stored debug data such as failed rawdata messages.
         */
        private String localStoragePassword;

        /**
         * <p>If specified, only the given rawdata entries will be included when logging or
         * storing rawdata to local storage path. This can be convenient if you for some
         * reason only want to include "whitelisted" data in logs or to local storage.</p>
         *
         * <p>Defaults to all items if not specified.</p>
         */
        private Set<String> includedRawdataEntries;

        public boolean isDryrun() {
            return Optional.ofNullable(dryrun).orElse(false);
        }
        public boolean isDevelopmentMode() {
            return Optional.ofNullable(developmentMode).orElse(false);
        }

        public boolean shouldLogFailedRawdata() {
            return Optional.ofNullable(logFailedRawdata).orElse(false);
        }
        public boolean shouldLogSkippedRawdata() {
            return Optional.ofNullable(logSkippedRawdata).orElse(false);
        }
        public boolean shouldLogAllRawdata() {
            return Optional.ofNullable(logAllRawdata).orElse(false);
        }
        public boolean shouldLogAllConverted() {
            return Optional.ofNullable(logAllConverted).orElse(false);
        }
        public boolean shouldStoreFailedRawdata() {
            return Optional.ofNullable(storeFailedRawdata).orElse(false);
        }
        public boolean shouldStoreSkippedRawdata() {
        return Optional.ofNullable(storeSkippedRawdata).orElse(false);
    }
        public boolean shouldStoreAllRawdata() {
            return Optional.ofNullable(storeAllRawdata).orElse(false);
        }
        public boolean shouldStoreAllConverted() {
            return Optional.ofNullable(storeAllConverted).orElse(false);
        }
    }

    @ConfigurationProperties("converter-settings")
    @Data
    public static class ConverterSettings extends ConfigElement {
        /**
         * The max number of records to convert before writing results to parquet.
         */
        private Long maxRecordsBeforeFlush;

        /**
         * The max number of seconds before writing results to parquet.
         */
        private Long maxSecondsBeforeFlush;

        /**
         * <p>The max number of records to convert. The converter job will be stopped
         * when reaching this count.</p>
         *
         * <p>Defaults to unlimited.</p>
         */
        private Long maxRecordsTotal;

        /**
         * <p>The number of rawdata messages to supply the RawdataConverter#init method.</p>
         *
         * <p>Defaults to 1</p>
         */
        private Integer rawdataSamples;

        /**
         * Set of RawdataMessages (comma-separated string of ULIDs) that will explicitly be skipped from being converted
         *
         * <p>Note that using this should be considered a "hack" and thus only as "last resort" or if you can accept the
         * accompanying technical debt.</p>
         */
        private Set<String> skippedMessages;
    }

    @ConfigurationProperties("rawdata-source")
    @Data
    public static class RawdataSourceRef extends ConfigElement {
        /**
         * References a named entry in the rawdata.sources config section of the application config.
         */
        private String name;

        /**
         * <p>Name of the rawdata stream written by the data collector.</p>
         *
         * <p>If the rawdata client provider is either GCS or filesystem, the topic will be the
         * same as the name of the directory that holds the avro files that contains rawdata.</p>
         */
        private String topic;

        /**
         * <p>The position of the rawdata stream that the rawdata converter should start reading from.</p>
         *
         * <p>One of:<ul>
         *   <li>LAST - after the last known (converted) position, meaning that the rawdata converter will determine the last known position from the target dataset. If the target dataset is not readable, then the converter will start from the beginning of the stream</li>
         *   <li>FIRST - always from the beginning of the stream. Note that this might result in duplicates if the dataset already exists.</li>
         *   <li>a specific RawdataMessage ULID</li>
         * </ul></p>
         */
        private String initialPosition;
    }

    @ConfigurationProperties("target-storage")
    @Data
    public static class TargetStorage extends ConfigElement {
        /**
         * The root of the dataset (the part the prefixes the path)
         */
        private String root;

        /**
         * <p>Canonical path used in all practical situations to reference a dataset.</p>
         *
         * <p>Note that this not the full or absolute path to a dataset, e.g. the path
         * section does not include root and version fragments.</p>
         */
        private String path;

        /**
         * <p>A unique string used to denote the version of the dataset.</p>
         *
         * <p>For all practical cases this should be a timestamp (milliseconds since the Unix epoch)</p>
         */
        private String version;

        /**
         * <p>Path to a GCS service account keyfile that grants write access to the target dataset.</p>
         *
         * <p>You will only need to specify this property if the target dataset is located in GCS AND you explicitly
         * want to use a service account keyfile instead of compute engine credentials. If the “rawdata client provider”
         * is GCS and this property is null, then it is assumed that compute engine credentials should be used.</p>
         */
        private String saKeyFile;
    }

    @ConfigurationProperties("target-dataset")
    @Data
    public static class TargetDataset extends ConfigElement {
        private Valuation valuation;
        private DatasetType type;
        private Boolean publishMetadata;

        public boolean shouldPublishMetadata() {
            return Optional.ofNullable(publishMetadata).orElse(false);
        }
    }

}
