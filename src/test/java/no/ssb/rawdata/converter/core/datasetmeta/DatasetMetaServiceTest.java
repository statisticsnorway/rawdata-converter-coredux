package no.ssb.rawdata.converter.core.datasetmeta;

import no.ssb.dapla.dataset.api.DatasetMeta;
import no.ssb.dlp.pseudo.core.PseudoFuncRule;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DatasetMetaServiceTest {

    @Test
    void publishDatasetMetaEvent_toJson_shouldIncludeDefaultValues() {
        PublishDatasetMetaEvent e = PublishDatasetMetaEvent.builder()
          .storageRoot("gs://blah")
          .storagePath("/kilde/foo")
          .storageVersion("1612430500000")
          .type(DatasetType.valueOf("BOUNDED"))
          .valuation(Valuation.valueOf("SENSITIVE"))
          .pseudoRules(List.of(new PseudoFuncRule("somename", "**/blah", "some-pseudofunc(param1,param2)")))
          .build();

        DatasetMeta meta = DatasetMetaService.datasetMetaOf(e);
        String metaJson = DatasetMetaService.datasetMetaJsonOf(meta);

        assertThat(metaJson).contains("\"valuation\": \"SENSITIVE\"");
        assertThat(metaJson).contains("\"state\": \"RAW\"");
        assertThat(metaJson).contains("\"type\": \"BOUNDED\"");
    }
}