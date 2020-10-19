package no.ssb.rawdata.converter.core.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.ArrayList;
import java.util.List;

public class AggregateSchemaBuilder {

    static class SchemaItem {
        public SchemaItem(String name, Schema schema) {
            this.name = name;
            this.schema = schema;
        }

        private final String name;
        private final Schema schema;
    }

    public AggregateSchemaBuilder(String namespace) {
        this.namespace = namespace;
    }

    private final String namespace;
    private final List<SchemaItem> schemas = new ArrayList<>();

    public AggregateSchemaBuilder schema(String name, Schema schema) {
        schemas.add(new SchemaItem(name, schema));
        return this;
    }

    public Schema build() {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("root").namespace(namespace).fields();
        schemas.forEach(item ->
          fields.name(item.name).type().optional().type(item.schema)
        );

        return fields.endRecord();
    }
}
