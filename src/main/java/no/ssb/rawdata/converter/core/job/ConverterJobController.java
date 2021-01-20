package no.ssb.rawdata.converter.core.job;

import com.fasterxml.jackson.databind.JsonNode;
import de.huxhorn.sulky.ulid.ULID;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import no.ssb.rawdata.converter.util.Json;

import java.util.Optional;
import java.util.stream.Collectors;

// TODO Make conditional on property
@Controller("/jobs")
@RequiredArgsConstructor
public class ConverterJobController {
    private final ConverterJobScheduler jobScheduler;

    /**
     * Schedule a converter job using overrides from the specified JSON
     */
    @Post(consumes = MediaType.APPLICATION_JSON)
    public void scheduleJob(StartConverterJobRequest request) {

        // Honor the activeByDefault flag if it is explicitly set, else assume we want to start the job immediately
        if (request.getJobConfig().getActiveByDefault() == null) {
            request.getJobConfig().setActiveByDefault(true);
        }
        jobScheduler.schedulePartial(request.getJobConfig(), request.getConverterConfigJson().orElse(null));
    }

/*
    TODO: Implement resume
    @Post("/{jobId}/resume")
    public void resumeJob(String jobId) {
        jobScheduler.resumeFromLast(ULID.parseULID(jobId));
    }
*/

    /**
     * List all converter job execution summaries
     */
    @Get("/execution-summary")
    public HttpResponse<String> getJobExecutionSummary() {
        return HttpResponse.ok(Json.prettyFrom(
          jobScheduler.getJobs().values().stream()
            .map(job -> job.getExecutionSummary())
            .collect(Collectors.toList()))
        );
    }

    /**
     * List effective converter job configurations for registered jobs
     */
    @Get("/config")
    public HttpResponse<String> getJobConfigs() {
        return HttpResponse.ok(Json.prettyFrom(
          jobScheduler.getJobs().values().stream()
            .collect(Collectors.toMap(
              ConverterJob::jobId,
              ConverterJob::getJobConfig
            )))
        );
    }

    /**
     * Pause all converter jobs
     */
    @Post("/pause")
    public void pauseAllJobs() {
        jobScheduler.pauseAll();
    }

    /**
     * Resume all converter jobs
     */
    @Post("/resume")
    public void resumeAllJobs() {
        jobScheduler.resumeAll();
    }

    /**
     * Stop all converter jobs
     */
    @Post("/stop")
    public void stopAllJobs() {
        jobScheduler.stopAll();
    }

    /**
     * Pause a specific converter job
     */
    @Post("/{jobId}/pause")
    public void pauseJob(String jobId) {
        jobScheduler.pause(ULID.parseULID(jobId));
    }

    /**
     * Resume a specific converter job
     */
    @Post("/{jobId}/resume")
    public void resumeJob(String jobId) {
        jobScheduler.resume(ULID.parseULID(jobId));
    }

    /**
     * Stop a specific converter job
     */
    @Post("/{jobId}/stop")
    public void stopJob(@PathVariable String jobId) {
        jobScheduler.stop(ULID.parseULID(jobId));
    }

    /**
     * Retrieve the effective job config for a specific job
     */
    @Get("/{jobId}/config")
    public HttpResponse<String> getJobConfig(String jobId) {
        return HttpResponse.ok(Json.prettyFrom(jobScheduler.getJob(ULID.parseULID(jobId)).getJobConfig()));
    }

    /**
     * <p>Retrieve the dataset metadata deduced for a specific job.
     * </p>
     * <p>This can be used as a baseline for manual publishing of dataset metadata.
     * </p>
     */
    @Get("/{jobId}/dataset-meta")
    public HttpResponse<String> getJobDatasetMeta(String jobId) {
        return HttpResponse.ok(Json.prettyFrom(jobScheduler.getJob(ULID.parseULID(jobId)).createDatasetMetadataEvent()));
    }

    /**
     * Retrieve the execution summary for a specific job
     */
    @Get("/{jobId}/execution-summary")
    public HttpResponse<String> getJobExecutionSummary(String jobId) {
        return HttpResponse.ok(Json.prettyFrom(jobScheduler.getJob(ULID.parseULID(jobId)).getExecutionSummary()));
    }

    @Data
    public static class StartConverterJobRequest {
        private ConverterJobConfig jobConfig;
        private JsonNode converterConfig;

        public Optional<String> getConverterConfigJson() {
            return converterConfig == null ? Optional.empty() : Optional.of(converterConfig.toString());
        }
    }

}
