package no.ssb.rawdata.converter.util;

import lombok.experimental.UtilityClass;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import org.apache.avro.Schema;

@UtilityClass
public class AvroSchemaUtil {

    public static Schema readAvroSchema(String schemaFileName) {
        try {
            return new Schema.Parser().parse(AvroSchemaUtil.class.getClassLoader().getResourceAsStream(schemaFileName));
        } catch (Exception e) {
            throw new RawdataConverterException("Unable to load avro schema from " + schemaFileName, e);
        }
    }

}
