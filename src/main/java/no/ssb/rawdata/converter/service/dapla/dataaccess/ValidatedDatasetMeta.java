package no.ssb.rawdata.converter.service.dapla.dataaccess;

import lombok.Value;

@Value
public class ValidatedDatasetMeta {
    private final byte[] content;
    private final byte[] signature;
}
