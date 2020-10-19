package no.ssb.rawdata.converter.core.convert;


import lombok.Getter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ConversionResult {

    private static final String SUCCESSFUL_CONVERSIONS_KEY = "successfulConversions";

    @Getter
    private final GenericRecord genericRecord;

    @Getter
    private final List<Exception> failures;

    @Getter
    private final Map<String, Object> properties;

    // We might also want to introduce an AtomicDouble counter, however for now our primary counting needs are integer based
    @Getter
    private final Map<String, AtomicLong> counters;

    /**
     * Returns the int value of a counter specified by name.
     *
     * @param key counter key name
     * @return counter value or 0 if the counter value does not exist. Will never throw an exception.
     */
    public int getCountAsInt(String key) {
        return counters.getOrDefault(SUCCESSFUL_CONVERSIONS_KEY, new AtomicLong()).intValue();
    }

    /**
     * Returns the long value of a counter specified by name.
     *
     * @param key counter key name
     * @return counter value or 0 if the counter value does not exist. Will never throw an exception.
     */
    public long getCountAsLong(String key) {
        return counters.getOrDefault(SUCCESSFUL_CONVERSIONS_KEY, new AtomicLong()).longValue();
    }

    /**
     * Returns the double value of a counter specified by name.
     *
     * @param key counter key name
     * @return counter value or 0 if the counter value does not exist. Will never throw an exception.
     */
    public double getCountAsDouble(String key) {
        return counters.getOrDefault(SUCCESSFUL_CONVERSIONS_KEY, new AtomicLong()).doubleValue();
    }

    /**
     * Get the number of successful conversions
     *
     * This method will be removed in a later version. Converters should manage counter keys on the outside and use
     * `appendCounter(MY_KEY)` and `getCountAsInt(MY_KEY)` instead.
     *
     * @return the number of successful conversions
     */
    @Deprecated
    public int successfulConversions() {
        return getCountAsInt(SUCCESSFUL_CONVERSIONS_KEY);
    }

    public static ConversionResultBuilder builder(GenericRecordBuilder recordBuilder) {
        return new ConversionResultBuilder(recordBuilder);
    }

    public static class ConversionResultBuilder {

        private GenericRecordBuilder recordBuilder;
        private List<Exception> failures = new ArrayList<>();
        private final Map<String, Object> properties = new HashMap<>();
        private final Map<String, AtomicLong> counters = new HashMap<>();

        public ConversionResultBuilder(GenericRecordBuilder recordBuilder) {
            this.recordBuilder = recordBuilder;
        }

        public ConversionResultBuilder withRecord(String fieldName, GenericRecord record) {
            this.recordBuilder.set(fieldName, record);
            return this;
        }

        public ConversionResultBuilder addFailure(Exception e) {
            this.failures.add(e);
            return this;
        }

        public ConversionResultBuilder addProperty(String key, Object value) {
            this.properties.put(key, value);
            return this;
        }

        public ConversionResultBuilder appendCounter(String key, long delta) {
            AtomicLong current = this.counters.getOrDefault(key, new AtomicLong());
            current.addAndGet(delta);
            counters.putIfAbsent(key, current);
            return this;
        }

        public ConversionResult build() {
            return new ConversionResult(this);
        }
    }

    private ConversionResult(ConversionResultBuilder builder) {
        this.genericRecord = builder.recordBuilder.build();
        this.failures = Collections.unmodifiableList(builder.failures);
        this.properties = builder.properties;
        this.counters = builder.counters;
    }
}
