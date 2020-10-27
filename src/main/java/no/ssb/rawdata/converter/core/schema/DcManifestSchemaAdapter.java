package no.ssb.rawdata.converter.core.schema;

import lombok.Value;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.converter.util.RawdataMessageAdapter;
import no.ssb.rawdata.converter.util.WordUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Deduce avro schema automatically based on one or more RawdataMessage samples.
 * Provides functionality to produce a dcManifest GenericRecord from a RawdataMessage.
 */
@Value
public class DcManifestSchemaAdapter {

    private static final String FIELDNAME_ULID = "ulid";
    private static final String FIELDNAME_POSITION = "position";
    private static final String FIELDNAME_TIMESTAMP = "timestamp";
    private static final String FIELDNAME_METADATA = "metadata";

    private final Schema dcManifestSchema;

    /** Map of metadata field names to avro schema field names */
    private final Map<String, String> propToAvroFieldNames;

    /**
     * Create a new DcManifest GenericRecord with data from the supplied RawdataMessage
     * @param rawdataMessage RawdataMessage to retrieve dcMetadata data from
     *
     * @return a new dcManifest GenericRecord
     */
    public GenericRecord newRecord(RawdataMessage rawdataMessage) {
        RawdataMessageAdapter msg = new RawdataMessageAdapter(rawdataMessage);

        List<GenericRecord> metadataItemRecords = new ArrayList<>();

        msg.getAllItemMetadata().values().forEach(m -> {
            GenericRecordBuilder metadataItemRecordBuilder = new GenericRecordBuilder(getMetadataItemSchema());
            Map<String, Object> metadata = m.getMetadataMap();
            propToAvroFieldNames.forEach((propName, avroFieldName) -> {
                metadataItemRecordBuilder.set(avroFieldName, metadata.get(propName));
            });
            metadataItemRecords.add(metadataItemRecordBuilder.build());
        });

        return new GenericRecordBuilder(dcManifestSchema)
          .set(FIELDNAME_ULID, rawdataMessage.ulid().toString())
          .set(FIELDNAME_POSITION, rawdataMessage.position())
          .set(FIELDNAME_TIMESTAMP, rawdataMessage.timestamp())
          .set(FIELDNAME_METADATA, metadataItemRecords)
          .build();
    }

    public Schema getMetadataItemSchema() {
        return dcManifestSchema.getField(FIELDNAME_METADATA).schema().getElementType();
    }

    public static DcManifestSchemaAdapter of(RawdataMessage sample) {
        return DcManifestSchemaAdapter.of(List.of(sample));
    }

    public static DcManifestSchemaAdapter of(Collection<RawdataMessage> samples) {
        Set<String> fields = uniqueMetadataFieldsOf(samples);
        Map<String, String> metadataFieldNames = fields.stream()
          .collect(Collectors.toMap(Function.identity(), WordUtil::toCamelCase));

        Schema metadataItemSchema = metadataItemSchemaOf(metadataFieldNames.values());
        return new DcManifestSchemaAdapter(dcManifestSchemaOf(metadataItemSchema), metadataFieldNames);
    }

    /**
     * Find all unique field names in all known metadata items
     *
     * @return a Set of unique fieldnames used by all known metadata items
     */
    private static Set<String> uniqueMetadataFieldsOf(Collection<RawdataMessage> rawdataMessageSamples) {
        Set<String> metadataFieldNames = new LinkedHashSet<>();
        rawdataMessageSamples.forEach(rawdataMessage -> {
            RawdataMessageAdapter msg = new RawdataMessageAdapter(rawdataMessage);
            msg.getAllItemMetadata().values().forEach(m -> metadataFieldNames.addAll(m.getMetadataMap().keySet()));
        });

        return metadataFieldNames;
    }

    private static Schema metadataItemSchemaOf(Collection<String> metadataFieldNames) {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(FIELDNAME_METADATA).fields();
        for (String fieldName : metadataFieldNames) {
            fieldAssembler.optionalString(fieldName);
        }
        return fieldAssembler.endRecord();
    }

    private static Schema dcManifestSchemaOf(Schema metadataItemSchema) {
        return SchemaBuilder.record("dcManifest")
          .fields()
          .requiredString(FIELDNAME_ULID)
          .requiredString(FIELDNAME_POSITION)
          .requiredString(FIELDNAME_TIMESTAMP)
          .name(FIELDNAME_METADATA).type().array().items(metadataItemSchema).noDefault()
          .endRecord();
    }

}
