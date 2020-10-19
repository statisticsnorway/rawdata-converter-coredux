package no.ssb.rawdata.converter.core.event;

import lombok.NonNull;
import lombok.Value;

@Value
public class FieldConvertedEvent {
    @NonNull
    private final String path;

    @NonNull
    private final Object value;
}
