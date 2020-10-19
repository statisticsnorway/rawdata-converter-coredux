package no.ssb.rawdata.converter.core.event;

import lombok.NonNull;
import lombok.Value;

import java.util.List;

@Value
public class FieldConversionWarningEvent {
    @NonNull
    private final String path;

    @NonNull
    private final List<String> warnings;
}
