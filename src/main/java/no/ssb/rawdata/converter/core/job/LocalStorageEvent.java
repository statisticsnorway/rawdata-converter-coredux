package no.ssb.rawdata.converter.core.job;

import lombok.Builder;
import lombok.Data;

import java.nio.file.Path;
import java.util.Map;

@Data
@Builder
public class LocalStorageEvent {
    private Path pathPrefix;
    private String fileGroupName;
    private Map<String, byte[]> files;
    private String localStoragePassword;
}
