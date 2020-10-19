package no.ssb.rawdata.converter.app;

import ch.qos.logback.classic.util.ContextInitializer;

public abstract class AbstractRawdataConverterApplication {
    protected AbstractRawdataConverterApplication() {}

    static {
        String logbackConfigurationFile = System.getenv("LOGBACK_CONFIGURATION_FILE");
        if (logbackConfigurationFile != null) {
            System.setProperty(ContextInitializer.CONFIG_FILE_PROPERTY, logbackConfigurationFile);
        }
    }
}
