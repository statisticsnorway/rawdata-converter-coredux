package no.ssb.rawdata.converter.core.job;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

@Data
@ConfigurationProperties("rawdata.converter.job-scheduler")
public class ConverterJobSchedulerConfig {

    private int maxConcurrentJobs = 1;

}
