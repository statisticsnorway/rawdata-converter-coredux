package no.ssb.rawdata.converter.core.storage;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.dapla.storage.client.ParquetGroupVisitor;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * {@link UlidVisitor} defines a parquet projection schema to read the 'ulid' column from converted records.
 * It also keeps track of the latest ulid found in all converted record.
 */
public class UlidVisitor implements ParquetGroupVisitor {

    public static final MessageType ULID_PROJECTION_SCHEMA = MessageTypeParser.parseMessageType(
            "message dapla.rawdata.root {\n" +
                    "  optional group manifest {\n" +
                    "    optional group collector {\n" +
                    "      required binary ulid (UTF8);\n" +
                    "    }\n" +
                    "  }\n" +
                    "}"
    );

    private ULID.Value latest;

    @Override
    public void visit(SimpleGroup value) {
        ULID.Value ulid = ULID.parseULID(value.getGroup(0, 0).getGroup(0, 0).getValueToString(0, 0));
        if (latest == null || latest.compareTo(ulid) < 1) {
            latest = ulid;
        }
    }

    public ULID.Value getLatest() {
        return latest;
    }
}
