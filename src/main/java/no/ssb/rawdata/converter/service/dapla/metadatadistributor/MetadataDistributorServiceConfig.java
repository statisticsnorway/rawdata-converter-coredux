package no.ssb.rawdata.converter.service.dapla.metadatadistributor;


import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
@ConfigurationProperties("services.dapla-metadata-distributor")
public class MetadataDistributorServiceConfig {

    public enum Impl {
        GRPC, HTTP, MOCK;
    }

    private Impl impl = Impl.MOCK;

    private String host;

    private Integer port;

    private String projectId;

    private String topic;

}
