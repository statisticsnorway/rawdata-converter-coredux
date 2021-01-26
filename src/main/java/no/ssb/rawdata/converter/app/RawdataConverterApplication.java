package no.ssb.rawdata.converter.app;

import ch.qos.logback.classic.util.ContextInitializer;
import no.ssb.rawdata.converter.util.MavenArtifactUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RawdataConverterApplication {
    protected RawdataConverterApplication() {}

    static {
        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        }

        Logger log = LoggerFactory.getLogger(RawdataConverterApplication.class);
        log.info("rawdata-converter-core version: {}", rawdataConverterCoreVersion());
    }

    public static String rawdataConverterCoreVersion() {
        return MavenArtifactUtil.findArtifactVersion("no.ssb.rawdata.converter", "rawdata-converter-coredux").orElse("unknown");
    }

}
