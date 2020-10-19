package no.ssb.rawdata.converter.core.rawdatasource;

import lombok.RequiredArgsConstructor;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.service.provider.api.ProviderConfigurator;

import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

@Singleton
@RequiredArgsConstructor
public class RawdataClientFactory {

    /**
     * All rawdata-source configs as defined in application.yml (rawdata-sources)
     */
    private final List<RawdataSourceConfig> rawdataSources;

    public RawdataClient rawdataClientOf(String rawdataSourceName) {
        RawdataSourceConfig rawdataSourceConfig = findRawdataSource(rawdataSourceName)
          .orElseThrow(() -> new RawdataSourceNotFoundException(rawdataSourceName));

        return rawdataClientOf(rawdataSourceConfig.getRawdataClient());
    }

    public RawdataClient rawdataClientOf(Properties rawdataClientConfig) {
        return rawdataClientOf(rawdataClientConfig.stringPropertyNames().stream()
          .collect(Collectors.toMap(
            Function.identity(), propKey -> rawdataClientConfig.getProperty(propKey)
          ))
        );
    }

    public RawdataClient rawdataClientOf(Map<String, String> rawdataClientConfig) {
        String provider = rawdataClientConfig.get("provider");
        return ProviderConfigurator.configure(rawdataClientConfig, provider, RawdataClientInitializer.class);
    }

    public Optional<RawdataSourceConfig> findRawdataSource(String name) {
        return rawdataSources.stream()
          .filter(job -> job.getName().equals(name))
          .findAny();
    }

    public static class RawdataSourceNotFoundException extends RawdataConverterException {
        public RawdataSourceNotFoundException(String rawdataSourceName) {
            super("No rawdata source '" + rawdataSourceName + "' has been defined." +
              " Make sure that you have a rawdata-sources." + rawdataSourceName + " section in your application.yml config");
        }
    }

}
