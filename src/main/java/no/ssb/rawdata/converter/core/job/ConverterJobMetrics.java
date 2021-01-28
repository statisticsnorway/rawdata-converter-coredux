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
import no.ssb.rawdata.converter.metrics.Metric;
import no.ssb.rawdata.converter.metrics.MetricName;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ConverterJobMetrics {

    @NonNull
    private final PrometheusMeterRegistry meterRegistry;

    @NonNull
    private final ConverterJobConfig jobConfig;

    public static final Metric JOB_INFO = new Metric(MetricName.JOB_INFO);
    public static final Metric RAWDATA_MESSAGES_TOTAL_SUCCESS = new Metric(MetricName.RAWDATA_MESSAGES_TOTAL, "result", "success");
    public static final Metric RAWDATA_MESSAGES_TOTAL_FAIL = new Metric(MetricName.RAWDATA_MESSAGES_TOTAL, "result", "fail");
    public static final Metric RAWDATA_MESSAGES_TOTAL_SKIP = new Metric(MetricName.RAWDATA_MESSAGES_TOTAL, "result", "skip");
    public static final Metric RAWDATA_MESSAGE_SIZE_BYTES = new Metric(MetricName.RAWDATA_MESSAGE_SIZE_BYTES);

    private final DistributionSummary rawdataMessageSizeSummary;

    Map<String, Counter> counters = new LinkedHashMap<>();

    public ConverterJobMetrics(@NonNull PrometheusMeterRegistry prometheusMeterRegistry, @NonNull ConverterJobConfig jobConfig) {
        this.meterRegistry = prometheusMeterRegistry;
        this.jobConfig = jobConfig;

        //Register all known metrics upfront
        incrementJobInfoCounter();
        incrementCounter(RAWDATA_MESSAGES_TOTAL_SUCCESS, 0);
        incrementCounter(RAWDATA_MESSAGES_TOTAL_FAIL, 0);
        incrementCounter(RAWDATA_MESSAGES_TOTAL_SKIP, 0);

        rawdataMessageSizeSummary = DistributionSummary.builder(RAWDATA_MESSAGE_SIZE_BYTES.getName())
          .description("Size of encountered rawdata messages")
          .baseUnit(BaseUnits.BYTES)
          .tags(correlationTagsOf(jobConfig))
          .register(this.meterRegistry);
    }

    /**
     * Retrieve micrometer tags used to correlate converter jobs
     */
    private static Tags correlationTagsOf(ConverterJobConfig jobConfig) {
        return Tags.of(
          Tag.of("job_id", jobConfig.getJobId().toString()),
          Tag.of("job.name", jobConfig.getJobName()),
          Tag.of("target.storage.path", jobConfig.getTargetStorage().getPath()),
          Tag.of("target.storage.version", jobConfig.getTargetStorage().getVersion())
        );
    }

    public void incrementCounter(Metric metric) {
        incrementCounter(metric.getFullName(), 1);
    }

    public void incrementCounter(Metric metric, double increment) {
        incrementCounter(metric.getFullName(), increment);
    }

    public void incrementCounter(String metric) {
        incrementCounter(metric, 1);
    }

    public void incrementJobInfoCounter() {
        Meter.Id meter = new Meter.Id(
          JOB_INFO.getName(),
          Tags.of(
            Tag.of("job.id", jobConfig.getJobId().toString()),
            Tag.of("job.name", jobConfig.getJobName()),
            Tag.of("converter.core", RawdataConverterApplication.rawdataConverterCoreVersion()),
            Tag.of("target.storage.root", jobConfig.getTargetStorage().getRoot()),
            Tag.of("target.storage.path", jobConfig.getTargetStorage().getPath()),
            Tag.of("target.storage.version", jobConfig.getTargetStorage().getVersion()),
            Tag.of("target.dataset.type", jobConfig.getTargetDataset().getType().toString()),
            Tag.of("target.dataset.valuation", jobConfig.getTargetDataset().getValuation().toString()),
            Tag.of("rawdata.source.name", jobConfig.getRawdataSource().getName()),
            Tag.of("rawdata.source.topic", jobConfig.getRawdataSource().getTopic()),
            Tag.of("rawdata.source.position.start", jobConfig.getRawdataSource().getInitialPosition()),
            Tag.of("dryrun", Boolean.toString(jobConfig.getDebug().isDryrun()))
          ),
          null,
          "Static information about a rawdata converter job",
          Meter.Type.COUNTER
        );

        incrementCounter(JOB_INFO.getFullName(), 1, () -> meterRegistry.newCounter(meter));
    }

    public void incrementCounter(String metric, double increment) {
        incrementCounter(metric, increment, () -> newCounterWithCorrelationTags(metric));
    }

    private void incrementCounter(String metric, double increment, Supplier<Counter> counterSupplier) {
        Counter counter = this.counters.computeIfAbsent(metric, k -> counterSupplier.get());//getOrDefault(metric, defaultCounter);
        counters.putIfAbsent(metric, counter);
        counter.increment(increment);
    }

    public Counter newCounterWithCorrelationTags(String metric) {
        Metric m = new Metric(metric);
        return meterRegistry.counter(m.getName(), correlationTagsOf(jobConfig).and(m.getTags()));
    }

    public void appendSkippedMessagesCount() {
        incrementCounter(RAWDATA_MESSAGES_TOTAL_SKIP);
    }

    public Map<String, Double> getExecutionSummaryMetrics() {
        return counters.entrySet().stream()
          .collect(Collectors.toMap(
            e -> e.getKey(),
            e -> e.getValue().count()
          ));
    }

    public double getRawdataMessagesProcessedTotal() {
        return countOf(RAWDATA_MESSAGES_TOTAL_SUCCESS) +
          countOf(RAWDATA_MESSAGES_TOTAL_SKIP);
    }

    private void appendRawdataMessageSize(RawdataMessage rawdataMessage) {
        double messageBytes = 0;
        for (byte[] bytes : rawdataMessage.data().values()) {
            messageBytes += bytes.length;
        }

        rawdataMessageSizeSummary.record(messageBytes);
    }

    public void appendConversionResult(ConversionResult conversionResult) {
        incrementCounter(RAWDATA_MESSAGES_TOTAL_SUCCESS);
        appendRawdataMessageSize(conversionResult.getRawdataMessage());

        conversionResult.getCounters().forEach((key, count) -> {
            incrementCounter(key, count.get());
        });
        if (!conversionResult.getFailures().isEmpty()) {
            incrementCounter(RAWDATA_MESSAGES_TOTAL_FAIL, conversionResult.getFailures().size());
        }
    }

    private double countOf(Metric m) {
        Counter c = counters.get(m.getFullName());
        return (c != null) ? c.count() : 0;
    }

}
