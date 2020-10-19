package no.ssb.rawdata.converter.core.rawdatasource;

import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.EachProperty;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Introspected;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Properties;

@Context
@Data @Accessors(chain = true)
@EachProperty(value = "rawdata.sources")
public class RawdataSourceConfig {

    public RawdataSourceConfig(@Parameter String name) {
        this.name = name;
    }

    private String name;
    private Encryption encryption = new Encryption();
    private Properties rawdataClient = new Properties();

    @Introspected
    private static abstract class ConfigElement { }

    @ConfigurationProperties("encryption")
    @Data
    public static class Encryption extends ConfigElement {
        private char[] key;
        private byte[] salt;
    }

}
