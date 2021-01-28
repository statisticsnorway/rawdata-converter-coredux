package no.ssb.rawdata.converter.core.convert;

import no.ssb.rawdata.converter.core.job.ConverterJobConfig;

public interface RawdataConverterFactory {

    /**
     * Instantiate application specific RawdataConverter
     *
     * @param jobConfig the job config, including any app specific configuration in the `app-config` section
     * @return a new RawdataConverter instance
     */
    RawdataConverter newRawdataConverter(ConverterJobConfig jobConfig);

}
