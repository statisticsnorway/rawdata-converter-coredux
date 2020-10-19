package no.ssb.rawdata.converter.core.convert;

import no.ssb.avro.convert.core.FieldDescriptor;
import no.ssb.avro.convert.core.ValueInterceptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// TODO: Rename this to something more intuitive, e.g. ValueConversionPostProcessorChain

/**
 * A chain of {@link ValueInterceptor}s that should be invoked serially after conversion.
 *
 * This can be used together with avro-buddy converters in order to perform multiple additional tasks such as
 * pseudonymization, validation, reporting, logging, etc
 *
 * Example: 1) Perform pseudonymization and 2) log schema metrics
 */
public class ValueInterceptorChain implements ValueInterceptor {
    private final List<ValueInterceptor> valueInterceptorChain = new ArrayList<>();

    public ValueInterceptorChain register(ValueInterceptor valueInterceptor) {
        valueInterceptorChain.add(valueInterceptor);
        return this;
    }

    public ValueInterceptorChain register(ValueInterceptor valueInterceptor, ValueInterceptor... valueInterceptors) {
        valueInterceptorChain.add(valueInterceptor);
        valueInterceptorChain.addAll(Arrays.asList(valueInterceptors));
        return this;
    }

    @Override
    public String intercept(FieldDescriptor field, String value) {
        for (ValueInterceptor vi : valueInterceptorChain) {
            value = vi.intercept(field, value);
        }
        return value;
    }
}
