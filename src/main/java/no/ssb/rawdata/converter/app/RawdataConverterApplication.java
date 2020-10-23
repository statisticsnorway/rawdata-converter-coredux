package no.ssb.rawdata.converter.app;

import ch.qos.logback.classic.util.ContextInitializer;
import lombok.extern.slf4j.Slf4j;
import no.ssb.rawdata.converter.util.MavenArtifactUtil;

@Slf4j
public abstract class RawdataConverterApplication {
    static {
        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        }
    }

    protected RawdataConverterApplication() {}

    private static void logStartupInfo() {
        log.info("rawdata-converter-core version: {}", MavenArtifactUtil.findArtifactVersion("no.ssb.rawdata.converter", "rawdata-converter-coredux").orElse("unknown"));
    }
}
