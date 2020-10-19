package no.ssb.rawdata.converter.core.datasetmeta;

import lombok.Getter;

@Getter
public enum Valuation {

    SENSITIVE(0),
    SHIELDED(1),
    INTERNAL(2),
    OPEN(3),
    UNRECOGNIZED(-1)
    ;

    private final int value;

    Valuation(int value) {
        this.value = value;
    }
}
