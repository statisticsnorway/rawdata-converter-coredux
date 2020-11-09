package no.ssb.rawdata.converter.service.dapla.metadatadistributor;

import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Requires;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Context
@RequiredArgsConstructor
@Slf4j
@Requires(property = MetadataDistributorServiceConfig.PREFIX + ".impl", value = "HTTP")
public class HttpMetadataDistributorService implements MetadataDistributorService {

    @NonNull
    private final MetadataDistributorServiceConfig config;

    private final MetadataDistributorHttpClient client;

    @Override
    public void publishFile(String fileUri) {
        client.notifyDataChanged(
          MetadataDistributorHttpClient.NotifyDataChangedRequest.builder()
            .projectId(config.getProjectId())
            .topicName(config.getTopic())
            .uri(fileUri)
            .build())
          .blockingGet(); // TODO: Don't block
    }

}
