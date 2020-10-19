package no.ssb.rawdata.converter.core.datasetmeta;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.Map;

@Value
@Builder
public class PublishDatasetMetaEvent {

    @NonNull
    private String storageRoot;

    @NonNull
    private String storagePath;

    @NonNull
    private String storageVersion;

    @NonNull
    private Valuation valuation;

    @NonNull
    private DatasetType type;

//    @NonNull TODO: Enable this!
    private Map<String, Object> pseudoRules;

}
