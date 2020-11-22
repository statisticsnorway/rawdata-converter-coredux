package no.ssb.rawdata.converter.core.job;

import avro.shaded.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import de.huxhorn.sulky.ulid.ULID;
import io.micronaut.context.event.ApplicationEventPublisher;
import io.reactivex.Flowable;
import io.reactivex.exceptions.UndeliverableException;
import io.reactivex.plugins.RxJavaPlugins;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dapla.dataset.uri.DatasetUri;
import no.ssb.dapla.storage.client.DatasetStorage;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.converter.core.convert.RawdataConverter;
import no.ssb.rawdata.converter.core.crypto.DecryptedRawdataMessage.DecryptRawdataMessageException;
import no.ssb.rawdata.converter.core.crypto.RawdataDecryptor;
import no.ssb.rawdata.converter.core.datasetmeta.DatasetType;
import no.ssb.rawdata.converter.core.datasetmeta.PublishDatasetMetaEvent;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.rawdata.converter.core.rawdatasource.RawdataConsumers;
import no.ssb.rawdata.converter.util.DatasetUriBuilder;
import no.ssb.rawdata.converter.util.Json;
import no.ssb.rawdata.converter.util.RawdataMessageAdapter;
import no.ssb.rawdata.converter.util.RuntimeVariables;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static no.ssb.rawdata.converter.util.RawdataMessageAdapter.posAndIdOf;

// TODO: Externalize execution metrics

@Slf4j
@Builder
public class ConverterJob {

    // TODO: Make this configurable
    private static final int TIMEOUT = 1 * 1000; // seconds

    private final ULID.Value jobId = new ULID().nextValue();
    private final ConverterJobRuntime runtime = new ConverterJobRuntime();
    private final Deque<RawdataMessage> lastRawdataMessages = new ArrayDeque<>();
    private final Map<String, Object> executionSummaryProperties = new LinkedHashMap<>();
    private final Map<String, AtomicLong> counters = new LinkedHashMap<>();

    @NonNull private final ConverterJobConfig jobConfig;
    @NonNull private final RawdataConverter rawdataConverter;
    @NonNull private final RawdataConsumers rawdataConsumers;
    @NonNull private final RawdataDecryptor rawdataDecryptor;
    @NonNull private final DatasetStorage datasetStorage;
    @NonNull private final ApplicationEventPublisher eventPublisher;

    static {
        // Handle errors that couldn't be emitted due to the downstream reaching its terminal state, or the cancellation
        // of a sequence about to emit an error. Ref: https://github.com/ReactiveX/RxJava/wiki/What's-different-in-2.0#error-handling
        RxJavaPlugins.setErrorHandler(e -> {
            if (e instanceof UndeliverableException) {
                e = e.getCause();
            }
            if (e instanceof DecryptRawdataMessageException) {
                // fine, a rawdata message could not be decrypted. possibly due to it not being encrypted.
                return;
            }
            log.error("Undeliverable exception received, not sure what to do ¯\\_(ツ)_/¯", e);
        });
    }

    public ULID.Value jobId() {
        return jobId;
    }

    ConverterJobRuntime runtime() {
        return runtime;
    }

    public void init() {
        log.info("Initialize converter job {}\n{}", jobId(), Json.prettyFrom(jobConfig));
        log.info("memory:\n{}", RuntimeVariables.memory());

        executionSummaryProperties.putIfAbsent("configuredStartPosition", jobConfig.getRawdataSource().getInitialPosition());
        executionSummaryProperties.putIfAbsent("actualStartPosition", rawdataConsumers.getMainInitialPosition());

        ConverterJobConfig.Debug debug = jobConfig.getDebug();

        if (debug.isDryrun()) {
            log.warn("Converter is running in dryrun mode. No records will be written.");
        }

        // TODO: Move these warnings to effective converter job config assembly?
        boolean isRawdataBeingLogged = debug.shouldLogAllRawdata() || debug.shouldLogFailedRawdata() || debug.shouldLogSkippedRawdata();
        if (isRawdataBeingLogged && !debug.isDevelopmentMode()) {
            log.warn("Converter is allowing rawdata content to be logged in clear text. This is NOT recommended for a production environment.");
        }
        boolean isRawdataBeingStored = debug.shouldStoreAllRawdata() || debug.shouldStoreFailedRawdata() || debug.shouldStoreSkippedRawdata();
        if (isRawdataBeingStored && debug.getLocalStoragePath() == null) {
            throw new IllegalStateException("Converter is configured to store rawdata messages, but no storage path is specified (missing '[job].debug.local-storage-path')");
        }

        rawdataConverter.init(sampleRawdataMessages());
        tryPublishDatasetMetadata();

        if (jobConfig.isActiveByDefault()) {
            this.start();
        }
    }

    public void start() {
        log.info("Start converter job {}", jobId());

        Schema targetAvroSchema = rawdataConverter.targetAvroSchema();

        executionSummaryProperties.putIfAbsent("startTimestamp", Instant.now().toString());
        runtime.start();

        processRawdataMessages(rawdataMessagesFlowOf(rawdataConsumers.getMainRawdataConsumer()), targetAvroSchema);
    }

    /**
     * Pause the converter job
     */
    public void pause() {
        log.info("Pause converter job {}", jobId);
        runtime.pause();
    }

    /**
     * Activate the converter job
     */
    public void resume() {
        log.info("Resume converter job {}", jobId);
        runtime.start();
    }

    public void stop() {
        log.info("Stop converter job {}", jobId);
        executionSummaryProperties.put("stopTimestamp", Instant.now().toString());
        runtime.stop();
        log.info("Converter job summary:\n{}", Json.prettyFrom(getExecutionSummary()));
        close();
    }

    // TODO: Implement close
    public void close() {
        // TODO: Close rawdata consumers?
    }

    private List<RawdataMessage> sampleRawdataMessages() {
        int sampleCount = Optional.ofNullable(jobConfig.getConverterSettings().getRawdataSamples()).orElse(0);
        return (sampleCount == 0) ? List.of() : rawdataMessagesListOf(rawdataConsumers.getSampleRawdataConsumer(), sampleCount);
    }

    PublishDatasetMetaEvent createDatasetMetadataEvent() {
        return PublishDatasetMetaEvent.builder()
          .storageRoot(jobConfig.getTargetStorage().getRoot())
          .storagePath(jobConfig.getTargetStorage().getPath())
          .storageVersion(jobConfig.getTargetStorage().getVersion())
          .valuation(jobConfig.getTargetDataset().getValuation())
          .type(jobConfig.getTargetDataset().getType())
          .pseudoRules(jobConfig.getPseudoRules())
          .build();
    }

    private void tryPublishDatasetMetadata() {
        PublishDatasetMetaEvent datasetMetaEvent = createDatasetMetadataEvent();
        if (jobConfig.getTargetDataset().shouldPublishMetadata()) {

            if (jobConfig.getDebug().isDryrun()) {
                log.info("Dataset metadata publishing is skipped when running in dryrun mode. Payload would be:\n{}", Json.prettyFrom(datasetMetaEvent));
            }
            else {
                eventPublisher.publishEvent(datasetMetaEvent);
            }
        }
        else {
            log.warn("Dataset metadata publishing is NOT done automatically for this converter job. You will have to trigger this manually in order " +
              "for the converted dataset to be discoverable. To enable automatic publishing on startup, set '[job].target-dataset.publish-metadata=true'. " +
              "You can use the following payload when triggering metadata publishing manually:\n{}", Json.prettyFrom(datasetMetaEvent)
            );
        }
    }

    // TODO: Test and validate this
    public void resumeFromLast() {
        if (runtime.isStarted()) {
            throw new ConverterJobException("Job " + jobId + "is already started. Must be in paused state in order to resume.");
        }
        if (runtime.isStopped()) {
            throw new ConverterJobException("Job " + jobId + " is terminated and can not be resumed");
        }

        RawdataMessage lastRawdataMessage = lastRawdataMessage().orElse(null);
        if (lastRawdataMessage != null) {
            log.info("Resuming rawdata message conversion from last message - " + posAndIdOf(lastRawdataMessage));
            rawdataConsumers.getMainRawdataConsumer().seek(lastRawdataMessage.timestamp());
            runtime.start();
        }
        else {
            log.warn("No last rawdata message to resume from. Doing nothing.");
        }
    }

    public ConverterJobConfig getJobConfig() {
        return jobConfig;
    }

    public Map<String, Object> getExecutionSummary() {

        double avgMessagesPerSecond = 0;
        try {
            avgMessagesPerSecond = (double) counters.get("processedMessagesCount").get() / runtime.getElapsedTimeInSeconds();
        }
        catch (Exception e) { /* OK to swallow */ }

        Map<String, Object> summary = new LinkedHashMap();
        summary.putAll(ImmutableMap.<String,Object>builder()
          .put("jobId", jobId().toString())
          .put("status", runtime.getState())
          .putAll(executionSummaryProperties)
          .put("currentPosition", posAndIdOf(lastRawdataMessage().orElse(null)))
          .put("totalExecutionTime", runtime.getElapsedTimeAsString())
          .put("avgMessagesPerSecond", avgMessagesPerSecond)
          .put("avgMessagesPerHour", avgMessagesPerSecond * 3600)
          .putAll(counters)
          .put("storageRoot", jobConfig.getTargetStorage().getRoot())
          .put("storagePath", jobConfig.getTargetStorage().getPath())
          .put("storageVersion", jobConfig.getTargetStorage().getVersion())
          .build());

        return summary;
    }

    List<RawdataMessage> rawdataMessagesListOf(RawdataConsumer rawdataConsumer, int maxSize) {
        List<RawdataMessage> rawdataMessages = new ArrayList<>();
        for (int count=0; count<maxSize; count++) {
            try {
                RawdataMessage rawdataMessage = rawdataConsumer.receive(TIMEOUT, TimeUnit.MILLISECONDS);

                if (rawdataMessage == null) {
                    break;
                }
                else {
                    rawdataMessages.add(rawdataDecryptor.tryDecrypt(rawdataMessage));
                }
            }
            catch (InterruptedException | RawdataClosedException e) {
                break;
            }
        }
        return rawdataMessages;
    }

    Flowable<RawdataMessage> rawdataMessagesFlowOf(RawdataConsumer rawdataConsumer) {

        return Flowable.generate(emitter -> {
            if (! runtime.isStarted()) {
                emitter.onComplete();
            }

            RawdataMessage message = rawdataConsumer.receive(TIMEOUT, TimeUnit.MILLISECONDS);

            if (message != null) {
                log.info("[{}] Process RawdataMessage - {}", jobId(), posAndIdOf(message));
                emitter.onNext(message);
            } else {
                if (jobConfig.getTargetDataset().getType() == DatasetType.BOUNDED) {
                    log.info("End of rawdata stream reached for BOUNDED dataset");
                    emitter.onComplete();
                    this.stop();
                }
                else {
                    log.debug("Waiting for rawdata. Sleeping for {} seconds.", TIMEOUT / 1000);
                }
            }
        });
    }

    void processRawdataMessages(Flowable<RawdataMessage> rawdataMessages, Schema targetAvroSchema) {

        long maxSecondsBeforeFlush = jobConfig.getConverterSettings().getMaxSecondsBeforeFlush();
        long maxRecordsBeforeFlush = jobConfig.getConverterSettings().getMaxRecordsBeforeFlush();

        while (!runtime.isStopped()) {
            if (runtime.isStarted()) {

                // Convert dryrun
                if (jobConfig.getDebug().getDryrun()) {
                    rawdataMessages
                      .window(maxSecondsBeforeFlush, TimeUnit.SECONDS, maxRecordsBeforeFlush, true)
                      .switchMapMaybe(
                        recordsWindow -> convertRecords(recordsWindow).lastElement()
                      )
                      .subscribe(
                        onNext -> {},
                        exception -> {
                            deactivateAndLogProcessingError("Error processing rawdata", lastRawdataMessage().orElse(null), exception);
                        },
                        () -> log.info("Rawdata stream completed")
                      );
                }

                // Convert and write
                else {
                    datasetStorage.writeDataUnbounded(
                      datasetUriOf(jobConfig.getTargetStorage()), // dataset to write to
                      targetAvroSchema, // avro schema
                      convertRecords(rawdataMessages), // map rawdata to avro records
                      maxSecondsBeforeFlush, TimeUnit.SECONDS, maxRecordsBeforeFlush // windowing criteria
                    ).subscribe(
                      onNext -> {},
                      exception -> {
                          deactivateAndLogProcessingError("Error processing rawdata", lastRawdataMessage().orElse(null), exception);
                      },
                      () -> log.info("Rawdata stream completed")
                    );
                }
            }
        }
    }

    void deactivateAndLogProcessingError(String errorMessage, RawdataMessage rawdataMessage, Throwable cause) {
        log.error(errorMessage + " - " + posAndIdOf(rawdataMessage) + ". Deactivating converter", cause);
        this.pause();
        if (rawdataMessage == null) {
            return;
        }

        if (jobConfig.getDebug().shouldLogFailedRawdata()) {
            log.info("RawdataMessage ({}):\n{}", posAndIdOf(rawdataMessage), RawdataMessageAdapter.toDebugString(rawdataMessage));
        }
        if (jobConfig.getDebug().shouldStoreFailedRawdata()) {
            storeFailedRawdataToFile(rawdataMessage, cause);
            log.info("Wrote failed rawdata message contents to local filesystem");
        }
    }

    private Flowable<GenericRecord> convertRecords(Flowable<RawdataMessage> rawdataMessages) {
        return rawdataMessages
                .map(rawdataDecryptor::tryDecrypt) // decrypt message data if encryption is configured
                .filter(rawdataMessage -> { // filter out records that should be skipped conversion
                    if (rawdataConverter.isConvertible(rawdataMessage)) {
                        return true;
                    }
                    else {
                        if (jobConfig.getDebug().shouldLogSkippedRawdata()) {
                            log.info("Skipped RawdataMessage ({}):\n{}", posAndIdOf(rawdataMessage), RawdataMessageAdapter.toDebugString(rawdataMessage));
                        }
                        if (jobConfig.getDebug().shouldStoreSkippedRawdata()) {
                            storeRawdataToFile(rawdataMessage, "skipped-rawdata");
                        }

                        appendCounter("skippedMessagesCount", 1);
                        return false;
                    }
                })
                .map(rawdataMessage -> { // optionally write record to file
                    if (jobConfig.getDebug().shouldStoreAllRawdata()) {
                        storeRawdataToFile(rawdataMessage, "rawdata");
                    }
                    if (jobConfig.getDebug().shouldLogAllRawdata()) {
                        log.info("RawdataMessage ({}):\n{}", posAndIdOf(rawdataMessage), RawdataMessageAdapter.toDebugString(rawdataMessage));
                    }
                    return rawdataMessage;
                })
                .map(rawdataMessage -> { // Convert records
                    lastRawdataMessages.push(rawdataMessage);
                    if (lastRawdataMessages.size() > 10) {
                        lastRawdataMessages.pollLast();
                    }

                    return rawdataConverter.convert(rawdataMessage);
                })
                .map(conversionResult -> { // Gather metrics
                    appendCounter("processedMessagesCount", 1);
                    conversionResult.getCounters().forEach((key, count) -> {
                        appendCounter(key, count.longValue());
                    });
                    if (! conversionResult.getFailures().isEmpty()) {
                        appendCounter("failedMessagesCount", conversionResult.getFailures().size());
                    }

                    GenericRecord record = conversionResult.getGenericRecord();
                    if (jobConfig.getDebug().shouldLogAllConverted()) {
                        log.info("Converted record:\n{}", record.toString());
                    }
                    if (jobConfig.getDebug().shouldStoreAllConverted()) {
                        storeGenericRecordToFile(record,  "converted/" + conversionResult.getRawdataMessage().position(), "converted.json");
                    }

                    return record;
                });
    }

    // TODO: Move the local storage stuff into a separate service

    private Path localStoragePath(String pathSuffix) {
        String root = Optional.ofNullable(jobConfig.getDebug().getLocalStoragePath()).orElse("/tmp");
        return Path.of(root, jobConfig.getRawdataSource().getTopic(), pathSuffix);
    }

    private void storeGenericRecordToFile(GenericRecord record, String pathSuffix, String filename) {
        Path targetPath = localStoragePath(pathSuffix);
        try {
            Files.createDirectories(targetPath);
            Files.write(targetPath.resolve(filename), Json.prettyFrom(record.toString()).getBytes());
        }
        catch (IOException ioe) {
            throw new RawdataConverterException("Error writing generic record to " + targetPath, ioe);
        }
    }

    private void storeFailedRawdataToFile(RawdataMessage rawdataMessage, Throwable cause) {
        storeRawdataToFile(rawdataMessage, "failed-rawdata", Map.of(
          "stacktrace.txt", Throwables.getStackTraceAsString(cause).getBytes(),
          "targetSchema.avsc", Json.prettyFrom(rawdataConverter.targetAvroSchema().toString()).getBytes(),
          "converterJobConfig.json", Json.prettyFrom(jobConfig).getBytes()
        ));
    }

    private void storeRawdataToFile(RawdataMessage rawdataMessage, String pathSuffix) {
        this.storeRawdataToFile(rawdataMessage, pathSuffix, null);
    }

    private void storeRawdataToFile(RawdataMessage rawdataMessage, String pathSuffix, Map<String, byte[]> additionalFiles) {
        Path targetPath = localStoragePath(pathSuffix);
        try {
            RawdataMessageAdapter.write(rawdataMessage, targetPath);
            appendCounter("rawdataFilesWritten", rawdataMessage.keys().size());

            if (additionalFiles != null) {
                for (Entry<String, byte[]> e : additionalFiles.entrySet()) {
                    Files.write(targetPath.resolve(rawdataMessage.position()).resolve(e.getKey()), e.getValue());
                }
            }
        }
        catch (IOException ioe) {
            throw new RawdataConverterException("Error writing rawdata message " + posAndIdOf(rawdataMessage) + " to " + targetPath, ioe);
        }
    }

    public void appendCounter(String key, long delta) {
        AtomicLong current = this.counters.getOrDefault(key, new AtomicLong());
        current.addAndGet(delta);
        counters.putIfAbsent(key, current);
    }

    private static DatasetUri datasetUriOf(ConverterJobConfig.TargetStorage storage) {
        return DatasetUriBuilder.of()
          .root(storage.getRoot())
          .path(storage.getPath())
          .version(storage.getVersion())
          .build();
    }

    public Optional<RawdataMessage> lastRawdataMessage() {
        return Optional.ofNullable(lastRawdataMessages.peekFirst());
    }
}
