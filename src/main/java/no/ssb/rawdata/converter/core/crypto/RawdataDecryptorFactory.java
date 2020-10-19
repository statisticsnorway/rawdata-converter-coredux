package no.ssb.rawdata.converter.core.crypto;

import lombok.RequiredArgsConstructor;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.rawdata.converter.core.rawdatasource.RawdataSourceConfig;

import javax.inject.Singleton;
import java.util.List;
import java.util.Optional;

@Singleton
@RequiredArgsConstructor
public class RawdataDecryptorFactory {

    /**
     * All rawdata-source configs as defined in application.yml (rawdata-sources)
     */
    private final List<RawdataSourceConfig> rawdataSources;

    public RawdataDecryptor rawdataDecryptorOf(String rawdataSourceName) {
        RawdataSourceConfig rawdataSourceConfig = findRawdataSource(rawdataSourceName)
          .orElseThrow(() -> new RawdataSourceNotFoundException(rawdataSourceName));

        return rawdataDecryptorOf(rawdataSourceConfig);
    }

    public RawdataDecryptor rawdataDecryptorOf(RawdataSourceConfig rawdataSourceConfig) {
        return new RawdataDecryptor(rawdataSourceConfig.getEncryption().getKey(), rawdataSourceConfig.getEncryption().getSalt());
    }

    private Optional<RawdataSourceConfig> findRawdataSource(String name) {
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
