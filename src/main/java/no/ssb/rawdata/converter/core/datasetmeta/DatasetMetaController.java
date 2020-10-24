package no.ssb.rawdata.converter.core.datasetmeta;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import no.ssb.dlp.pseudo.core.PseudoFuncRule;

import java.util.List;

@Controller("/dataset-meta")
@RequiredArgsConstructor
@Slf4j
public class DatasetMetaController {

    private final DatasetMetaService datasetMetaService;

    @Post(consumes = MediaType.APPLICATION_JSON)
    public HttpResponse publishDatasetMeta(DatasetMetaDto datasetMeta) {
        try {
            datasetMetaService.onPublishDatasetMeta(datasetMeta.toEvent());
            return HttpResponse.ok();
        }
        catch (DatasetMetaService.DatasetMetaPublishException e) {
            log.error("Unable to publish dataset metadata", e);
            return HttpResponse.serverError(e.getMessage());
        }
    }

    @Data
    public static class DatasetMetaDto {
        private String storageRoot;
        private String storagePath;
        private String storageVersion;
        private Valuation valuation;
        private DatasetType type;
        private List<PseudoFuncRule> pseudoRules;

        public PublishDatasetMetaEvent toEvent() {
            return new PublishDatasetMetaEvent(
              storageRoot, storagePath, storageVersion, valuation, type, pseudoRules);
        }
    }
}
