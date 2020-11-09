package no.ssb.rawdata.converter.service.dapla.metadatadistributor;


import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
@ConfigurationProperties(MetadataDistributorServiceConfig.PREFIX)
public class MetadataDistributorServiceConfig {

    public static final String PREFIX = "services.dapla-metadata-distributor";

    public enum Impl {
        GRPC, HTTP, MOCK;
    }

    private Impl impl = Impl.MOCK;

    private String host;

    private Integer port;

    private String projectId;

    private String topic;

}
