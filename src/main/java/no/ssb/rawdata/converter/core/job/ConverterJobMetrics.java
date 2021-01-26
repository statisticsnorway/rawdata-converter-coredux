package no.ssb.rawdata.converter.core.job;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.NonNull;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.converter.app.RawdataConverterApplication;
import no.ssb.rawdata.converter.core.convert.ConversionResult;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static no.ssb.rawdata.converter.core.job.ConverterJobMetrics.Metric.CONVERTER_RAWDATA_MESSAGES_TOTAL;
import static no.ssb.rawdata.converter.core.job.ConverterJobMetrics.Metric.CONVERTER_RAWDATA_MESSAGE_SIZE_BYTES;

public class ConverterJobMetrics {

    @NonNull private final PrometheusMeterRegistry meterRegistry;

    static class Metric {
        public static final String CONVERTER_JOB_INFO = "converter_job_info";
        public static final String CONVERTER_RAWDATA_MESSAGE_SIZE_BYTES = "converter_rawdata_message_size_bytes";
        public static final String CONVERTER_RAWDATA_MESSAGES_TOTAL = "converter_rawdata_messages_total";
        public static final String CONVERTER_RAWDATA_RECORDS_TOTAL = "converter_rawdata_records_total";
        public static final String CONVERTER_RAWDATA_FIELDS_TOTAL = "converter_rawdata_fields_total";
    }

    private final Map<String, AtomicLong> counters = new LinkedHashMap<>();
    private final Instant createdTimestamp = Instant.now();
    private Instant lastUpdateTimestamp;

//    private final Counter converterJobInfoCounter;
    private final Counter rawdataMessagesTotalSuccessCounter;
    private final Counter rawdataMessagesTotalFailCounter;
    private final Counter rawdataMessagesTotalSkipCounter;

    private final DistributionSummary rawdataMessageSizeSummary;

//    private final Counter rawdataRecordsTotalSuccessCounter;
//    private final Counter rawdataRecordsTotalFailCounter;
//    private final Counter rawdataRecordsTotalSkipCounter;

//    private final Counter rawdataFieldsTotalPseudo;
//    private final Counter rawdataFieldsTotalPlain;

    public ConverterJobMetrics(@NonNull PrometheusMeterRegistry prometheusMeterRegistry) {
        this.meterRegistry = prometheusMeterRegistry;
        this.lastUpdateTimestamp = createdTimestamp;

        rawdataMessagesTotalSuccessCounter = this.meterRegistry.counter(CONVERTER_RAWDATA_MESSAGES_TOTAL, "result", "success");
        rawdataMessagesTotalFailCounter = this.meterRegistry.counter(CONVERTER_RAWDATA_MESSAGES_TOTAL, "result", "fail");
        rawdataMessagesTotalSkipCounter = this.meterRegistry.counter(CONVERTER_RAWDATA_MESSAGES_TOTAL, "result", "skip");

        rawdataMessageSizeSummary = DistributionSummary.builder(CONVERTER_RAWDATA_MESSAGE_SIZE_BYTES)
          .description("Size of encountered rawdata messages")
          .baseUnit(BaseUnits.BYTES)
          .register(this.meterRegistry);
    }

    private static Meter.Id converterJobInfoMeterOf(ConverterJob job) {
        return new Meter.Id(
          Metric.CONVERTER_JOB_INFO,
          Tags.of(
            Tag.of("job_name", job.getJobConfig().getName()),
            Tag.of("job_id", job.jobId().toString()),
            Tag.of("converter_name", job.getRawdataConverter().getClass().getSimpleName()),
            Tag.of("converter_core", RawdataConverterApplication.rawdataConverterCoreVersion()),
            Tag.of("target_storage_root", job.getJobConfig().getTargetStorage().getRoot()),
            Tag.of("target_storage_path", job.getJobConfig().getTargetStorage().getPath()),
            Tag.of("target_storage_version", job.getJobConfig().getTargetStorage().getVersion()),
            Tag.of("target_dataset_type", job.getJobConfig().getTargetDataset().getType().toString()),
            Tag.of("target_dataset_valuation", job.getJobConfig().getTargetDataset().getValuation().toString()),
            Tag.of("rawdata_source_name", job.getJobConfig().getRawdataSource().getName()),
            Tag.of("rawdata_source_topic", job.getJobConfig().getRawdataSource().getTopic()),
            Tag.of("rawdata_source_initial_position", job.getJobConfig().getRawdataSource().getInitialPosition()),
            Tag.of("debug_dryrun", Boolean.toString(job.getJobConfig().getDebug().isDryrun()))
          ),
          null,
          "Static information about a rawdata converter job",
          Meter.Type.COUNTER
        );
    }

    private void appendCounter(String key, long delta) {
        AtomicLong current = this.counters.getOrDefault(key, new AtomicLong());
        current.addAndGet(delta);
        counters.putIfAbsent(key, current);
    }

    public void appendSkippedMessagesCount() {
        rawdataMessagesTotalSkipCounter.increment();
        appendCounter("skippedMessagesCount", 1);
    }

    public Map<String, AtomicLong> getCounters() {
        return Map.copyOf(counters);
    }

    public long getProcessedMessagesCount() {
        return counters.getOrDefault("processedMessagesCount", new AtomicLong()).get();
    }

    public long getEffectiveExecutionTimeInSeconds() {
        return Duration.between(createdTimestamp, lastUpdateTimestamp).toSeconds();
    }

    public double getAverageMessagesPerSecond() {
        try {
            return (double) getProcessedMessagesCount() / getEffectiveExecutionTimeInSeconds();
        } catch (Exception e) {
            return 0;
        }
    }

    public double getAverageMessagesPerHour() {
        return getAverageMessagesPerSecond() * 3600;
    }

    public void appendConverterJob(ConverterJob job) {
        this.meterRegistry.newCounter(converterJobInfoMeterOf(job)).increment();
    }

    private void appendRawdataMessageSize(RawdataMessage rawdataMessage) {
        double messageBytes = 0;
        for (byte[] bytes : rawdataMessage.data().values()) {
            messageBytes += bytes.length;
        }

        rawdataMessageSizeSummary.record(messageBytes);
    }

    public void appendConversionResult(ConversionResult conversionResult) {
        appendCounter("processedMessagesCount", 1);
        rawdataMessagesTotalSuccessCounter.increment();
        appendRawdataMessageSize(conversionResult.getRawdataMessage());

        conversionResult.getCounters().forEach((key, count) -> {
            appendCounter(key, count.longValue());
        });
        if (!conversionResult.getFailures().isEmpty()) {
            rawdataMessagesTotalFailCounter.increment(conversionResult.getFailures().size());
            appendCounter("failedMessagesCount", conversionResult.getFailures().size());
        }
        lastUpdateTimestamp = Instant.now();
    }

}
