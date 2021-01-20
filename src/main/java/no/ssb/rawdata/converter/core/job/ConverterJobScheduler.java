package no.ssb.rawdata.converter.core.job;

import de.huxhorn.sulky.ulid.ULID;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.Async;
import io.micronaut.scheduling.annotation.ExecuteOn;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.rawdata.converter.core.convert.RawdataConverterFactory;
import no.ssb.rawdata.converter.core.crypto.RawdataDecryptorFactory;
import no.ssb.rawdata.converter.core.rawdatasource.RawdataConsumerFactory;
import no.ssb.rawdata.converter.core.storage.DatasetStorageFactory;
import no.ssb.rawdata.converter.core.storage.StorageType;

import javax.inject.Singleton;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@Slf4j
@RequiredArgsConstructor
public class ConverterJobScheduler {

    private final Map<ULID.Value, ConverterJob> jobs = new ConcurrentHashMap<>();

    private final ConverterJobSchedulerConfig jobSchedulerConfig;
    private final ConverterJobConfigFactory effectiveConverterJobConfigFactory;
    private final RawdataConverterFactory rawdataConverterFactory;
    private final RawdataConsumerFactory rawdataConsumerFactory;
    private final RawdataDecryptorFactory rawdataDecryptorFactory;
    private final DatasetStorageFactory datasetStorageFactory;
    private final ApplicationEventPublisher eventPublisher;

    public void schedulePartial(ConverterJobConfig partialJobConfig, String converterConfigJson) {
        ConverterJobConfig jobConfig = effectiveConverterJobConfigFactory.effectiveConverterJobConfigOf(partialJobConfig);
        this.schedule(jobConfig, converterConfigJson);
    }

    public void schedule(ConverterJobConfig jobConfig) {
        this.schedule(jobConfig, null);
    }

    @Async
    @ExecuteOn(TaskExecutors.IO)
    public void schedule(ConverterJobConfig jobConfig, String converterConfigJson) {
        if (canAcceptJobs()) {
            ConverterJob job = ConverterJob.builder()
              .jobConfig(jobConfig)
              .rawdataConverter(rawdataConverterFactory.newRawdataConverter(jobConfig, converterConfigJson))
              .rawdataConsumers(rawdataConsumerFactory.rawdataConsumersOf(jobConfig))
              .rawdataDecryptor(rawdataDecryptorFactory.rawdataDecryptorOf(jobConfig.getRawdataSource().getName())) //TODO: Support rawdataDecryptor=null
              .datasetStorage(datasetStorageFactory.datasetStorageOf(StorageType.of(jobConfig.getTargetStorage().getRoot()), jobConfig.getTargetStorage().getSaKeyFile()))
              .localStorage(new ConverterJobLocalStorage(jobConfig, eventPublisher))
              .eventPublisher(eventPublisher)
              .build();

            jobs.put(job.jobId(), job);
            job.init();
        }
        else {
            throw new ConverterJobException("Not ready to start new converter job - request was ignored. Jobs started=" + jobs.size() + ", max jobs=" + jobSchedulerConfig.getMaxConcurrentJobs());
        }
    }

    public void resumeFromLast(ULID.Value jobId) {
        getJob(jobId).resumeFromLast();
    }

    public Map<ULID.Value, ConverterJob> getJobs() {
        return jobs;
    }

    public ConverterJob getJob(ULID.Value jobId) {
        return Optional.ofNullable(jobs.get(jobId))
          .orElseThrow(() -> new NoSuchElementException("Unable to find job with id=" + jobId));
    }

    public boolean canAcceptJobs() {
        long startedJobsCount = jobs.values().stream().filter(job -> ! job.runtime().isStopped()).count();
        return startedJobsCount < jobSchedulerConfig.getMaxConcurrentJobs();
    }

    public void resume(ULID.Value jobId) {
        getJob(jobId).resume();
    }

    public void resumeAll() {
        jobs.values().stream().filter(job -> job.runtime().isPaused()).forEach(job -> job.resume());
    }

    public void pause(ULID.Value jobId) {
        getJob(jobId).pause();
    }

    public void pauseAll() {
        jobs.values().stream().filter(job -> job.runtime().isPauseable()).forEach(job -> job.pause());
    }

    public void stop(ULID.Value jobId) {
        getJob(jobId).stop();
    }

    public void stopAll() {
        jobs.values().stream().filter(job -> ! job.runtime().isStopped()).forEach(job -> job.stop());
    }

}
