package no.ssb.rawdata.converter.core.job;

import io.micronaut.runtime.event.ApplicationStartupEvent;
import io.micronaut.runtime.event.annotation.EventListener;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

@Singleton
@RequiredArgsConstructor
@Slf4j
public class PredefinedConverterJobs {

    @NonNull
    private final ConverterJobConfigFactory effectiveConverterJobConfigFactory;

    @NonNull
    private final ConverterJobScheduler converterJobScheduler;

    /**
     * A RawdataConverter will be registered for each predefined job
     */
    @EventListener
    public void onStartup(ApplicationStartupEvent e) {
        PredefinedConverterJobConfigs jobConfigs = effectiveConverterJobConfigFactory.predefinedJobs();

        if (jobConfigs.isEmpty()) {
            log.info("No predefined converter jobs have been configured.");
        }
        else {
            // Start each job that is configured to be active by default
            jobConfigs.values().stream()
              .filter(ConverterJobConfig::isActiveByDefault)
              .forEach(converterJobScheduler::schedule);
        }
    }

}
