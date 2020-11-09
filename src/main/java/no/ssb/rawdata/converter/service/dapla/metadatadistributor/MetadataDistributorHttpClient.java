package no.ssb.rawdata.converter.service.dapla.metadatadistributor;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.Single;
import lombok.Builder;
import lombok.Data;

import javax.validation.constraints.NotBlank;

@Client("${services.dapla-metadata-distributor.host}:${services.dapla-metadata-distributor.port}")
@Requires(property = MetadataDistributorServiceConfig.PREFIX + ".impl", value = "HTTP")
public interface MetadataDistributorHttpClient {

    @Post("/rpc/MetadataDistributorService/dataChanged")
    Single<String> notifyDataChanged(@Body NotifyDataChangedRequest req);

    @Data
    @Builder
    class NotifyDataChangedRequest {
        String projectId;
        String topicName;
        String uri;
    }

}
