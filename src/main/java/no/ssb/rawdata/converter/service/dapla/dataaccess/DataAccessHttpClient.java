package no.ssb.rawdata.converter.service.dapla.dataaccess;

import io.micronaut.context.annotation.Requires;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.annotation.Client;
import io.reactivex.Single;
import lombok.Builder;
import lombok.Data;
import no.ssb.rawdata.converter.service.dapla.metadatadistributor.MetadataDistributorServiceConfig;

import javax.validation.constraints.NotBlank;

@Client("${services.dapla-data-access.host}:${services.dapla-data-access.port}")
@Requires(property = DataAccessServiceConfig.PREFIX + ".impl", value = "HTTP")
public interface DataAccessHttpClient {

    @Post("/rpc/DataAccessService/writeLocation")
    Single<WriteLocationResponse> writeLocation(@Body WriteLocationRequest req);

    @Data
    @Builder
    class WriteLocationRequest {
        private String metadataJson;
    }

    @Data
    class WriteLocationResponse {
        private String validMetadataJson;
        private String metadataSignature;
    }

}
