package no.ssb.rawdata.converter.service.dapla.dataaccess;


import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

import javax.validation.constraints.NotNull;

@Data
@ConfigurationProperties(DataAccessServiceConfig.PREFIX)
public class DataAccessServiceConfig {

    public static final String PREFIX = "services.dapla-data-access";

    public enum Impl {
        GRPC, HTTP, MOCK;
    }

    private Impl impl = Impl.MOCK;

    private String host;

    private Integer port;

    private Boolean useGrpc;
}
