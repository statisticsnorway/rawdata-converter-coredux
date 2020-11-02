package no.ssb.rawdata.converter.service.dapla.metadatadistributor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockMetadataDistributorService implements MetadataDistributorService{

    @Override
    public void publishFile(String fileUri) {
        log.warn("MockMetadataDistributorService - publishFile {} - NOTHING DONE", fileUri);
    }
}
