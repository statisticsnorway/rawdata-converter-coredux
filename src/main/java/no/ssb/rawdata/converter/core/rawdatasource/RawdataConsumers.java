package no.ssb.rawdata.converter.core.rawdatasource;

import lombok.Builder;
import lombok.Value;
import no.ssb.rawdata.api.RawdataConsumer;

@Value
@Builder
public class RawdataConsumers {
    /**
     * Rawdata consumer with starting position at initialPosition. This is the consumer from which rawdata messages
     * are streamed converted.
     */
    private final RawdataConsumer mainRawdataConsumer;

    /**
     * Rawdata consumer with starting position at start of topic. This consumer can be used to inspect/sample
     * rawdata messages in advance of conversion, in order to determine stuff such as target avro schema, etc.
     */
    private final RawdataConsumer sampleRawdataConsumer;

    /**
     * The actual initial position of the mainRawdataConsumer
     */
    private final String mainInitialPosition;

}
