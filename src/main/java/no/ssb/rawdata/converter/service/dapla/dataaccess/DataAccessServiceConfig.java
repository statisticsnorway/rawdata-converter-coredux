package no.ssb.rawdata.converter.service.dapla.dataaccess;


import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
@ConfigurationProperties("services.dapla-data-access")
public class DataAccessServiceConfig {

    @NotNull
    private String host;

    @NotNull
    private Integer port;

}
