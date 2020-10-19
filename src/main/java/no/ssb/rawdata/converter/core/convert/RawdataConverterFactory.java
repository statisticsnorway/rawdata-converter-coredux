package no.ssb.rawdata.converter.core.convert;

import no.ssb.rawdata.converter.core.job.ConverterJobConfig;

public interface RawdataConverterFactory {
    RawdataConverter newRawdataConverter(ConverterJobConfig jobConfig, String converterConfigJson);
}
