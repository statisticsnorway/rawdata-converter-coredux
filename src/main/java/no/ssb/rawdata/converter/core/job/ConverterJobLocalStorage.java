package no.ssb.rawdata.converter.core.job;

import com.google.common.collect.Sets;
import io.micronaut.context.event.ApplicationEventPublisher;
import lombok.RequiredArgsConstructor;
import no.ssb.rawdata.api.RawdataMessage;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@RequiredArgsConstructor
public class ConverterJobLocalStorage {
    private final ConverterJobConfig jobConfig;
    private final ApplicationEventPublisher eventPublisher;

    public void storeRawdataToFile(RawdataMessage rawdataMessage, String pathSuffix) {
        this.storeRawdataToFile(rawdataMessage, pathSuffix, null);
    }

    public void storeRawdataToFile(RawdataMessage rawdataMessage, String pathSuffix, Map<String, byte[]> additionalFiles) {
        Map<String, byte[]> files = new HashMap<>();
        for (String entryId : whitelistedRawdataEntries(rawdataMessage)) {
            files.put(entryId, rawdataMessage.get(entryId));
        }

        if (additionalFiles != null && ! additionalFiles.isEmpty()) {
            files.putAll(additionalFiles);
        }

        storeToFile(pathSuffix, rawdataMessage.position(), files);
    }

    public void storeToFile(String pathSuffix, String fileGroup, Map<String, byte[]> files) {
        String root = Optional.ofNullable(jobConfig.getDebug().getLocalStoragePath()).orElse("/tmp");
        eventPublisher.publishEvent(LocalStorageEvent.builder()
          .pathPrefix(Path.of(root, jobConfig.getRawdataSource().getTopic(), pathSuffix))
          .files(files)
          .fileGroupName(fileGroup)
          .localStoragePassword(jobConfig.getDebug().getLocalStoragePassword())
          .build());
    }

    private Set<String> whitelistedRawdataEntries(RawdataMessage rawdataMessage) {
        Set<String> whitelist = jobConfig.getDebug().getIncludedRawdataEntries();
        if (whitelist != null && !whitelist.isEmpty()) {
            return Sets.intersection(rawdataMessage.keys(), whitelist);
        }
        else {
            // allow all if no whitelist is specified
            return rawdataMessage.keys();
        }
    }

}
