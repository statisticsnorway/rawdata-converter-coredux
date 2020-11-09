package no.ssb.rawdata.converter.service.dapla.metadatadistributor;

import io.micronaut.context.annotation.Requires;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;

@Slf4j
@Singleton
@Requires(property = MetadataDistributorServiceConfig.PREFIX + ".impl", value = "MOCK")
public class MockMetadataDistributorService implements MetadataDistributorService{

    @Override
    public void publishFile(String fileUri) {
        log.warn("MockMetadataDistributorService - publishFile {} - NOTHING DONE", fileUri);
    }

}
