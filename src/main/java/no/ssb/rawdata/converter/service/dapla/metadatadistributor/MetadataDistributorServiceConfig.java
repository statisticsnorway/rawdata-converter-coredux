package no.ssb.rawdata.converter.service.dapla.metadatadistributor;


import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
@ConfigurationProperties("services.dapla-metadata-distributor")
public class MetadataDistributorServiceConfig {

    @NotNull
    private String host;

    @NotNull
    private Integer port;

    @NotNull
    private String projectId;

    @NotNull
    private String topic;

}
