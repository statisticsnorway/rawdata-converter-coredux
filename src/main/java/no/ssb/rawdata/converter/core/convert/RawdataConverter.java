package no.ssb.rawdata.converter.core.convert;

import no.ssb.rawdata.api.RawdataMessage;
import org.apache.avro.Schema;

import java.util.Collection;

public interface RawdataConverter {

    /**
     * Perform initialization and necessary preparations of the converter, such as calculating targetAvroSchema based on
     * sample RawdataMessages.
     *
     * @param sampleRawdataMessages
     */
    void init(Collection<RawdataMessage> sampleRawdataMessages);

    /**
     * Convert a RawdataMessage to a GenericRecord, according to the Avro schema specified by {@link #targetAvroSchema}
     *
     * @return an Avro GenericRecord
     */
    ConversionResult convert(RawdataMessage rawdataMessage);

    /**
     * @return the Avro schema that the converted GenericRecord will adhere to
     */
    Schema targetAvroSchema();

    /**
     * Checks if the converter should convert the supplied RawdataMessage
     *
     * @return true iff the converter should process the supplied RawdataMessage. If false, the RawdataMessage is skipped.
     */
    boolean isConvertible(RawdataMessage rawdataMessage);

}
