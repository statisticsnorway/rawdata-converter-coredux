package no.ssb.rawdata.converter.core.datasetmeta;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.core.MediaType;

@Controller("/dataset-meta")
@RequiredArgsConstructor
@Slf4j
public class DatasetMetaController {

    private final DatasetMetaService datasetMetaService;

    @Post(consumes = MediaType.APPLICATION_JSON)
    public HttpResponse publishDatasetMeta(PublishDatasetMetaEvent datasetMeta) {
        try {
            datasetMetaService.onPublishDatasetMeta(datasetMeta);
            return HttpResponse.ok();
        }
        catch (DatasetMetaService.DatasetMetaPublishException e) {
            log.error("Unable to publish dataset metadata", e);
            return HttpResponse.serverError(e.getMessage());
        }
    }

}
