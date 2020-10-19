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
        private Boolean dryrun;
        private Boolean developmentMode;
        private Boolean logRawdataContentAllowed;
        private Boolean storeFailedMessages;
        private String failedMessagesStoragePath;
        private Boolean storeAllMessages;
        private String allMessagesStoragePath;

        public boolean isDryrun() {
            return Optional.ofNullable(dryrun).orElse(false);
        }
        public boolean isDevelopmentMode() {
            return Optional.ofNullable(developmentMode).orElse(false);
        }
        public boolean isLogRawdataContentAllowed() {
            return Optional.ofNullable(logRawdataContentAllowed).orElse(false);
        }
        public boolean shouldStoreFailedMessages() {
            return Optional.ofNullable(storeFailedMessages).orElse(false);
        }
        public boolean shouldStoreAllMessages() {
            return Optional.ofNullable(storeAllMessages).orElse(false);
        }
    }

    @ConfigurationProperties("converter-settings")
    @Data
    public static class ConverterSettings extends ConfigElement {
        private Long maxRecordsBeforeFlush;
        private Long maxSecondsBeforeFlush;
        private Integer rawdataSamples;
    }

    @ConfigurationProperties("rawdata-source")
    @Data
    public static class RawdataSourceRef extends ConfigElement {
        private String name;
        private String topic;
        private String initialPosition;
    }

    @ConfigurationProperties("target-storage")
    @Data
    public static class TargetStorage extends ConfigElement {
        private String root;
        private String path;
        private String version;
        private String saKeyFile; // If root is GCS and this is null, then assume compute engine credentials to be used
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
