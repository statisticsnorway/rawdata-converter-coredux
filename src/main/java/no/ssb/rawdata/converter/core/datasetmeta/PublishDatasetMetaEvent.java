package no.ssb.rawdata.converter.core.datasetmeta;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import no.ssb.dlp.pseudo.core.PseudoFuncRule;

import java.util.List;

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

    private List<PseudoFuncRule> pseudoRules;

}
