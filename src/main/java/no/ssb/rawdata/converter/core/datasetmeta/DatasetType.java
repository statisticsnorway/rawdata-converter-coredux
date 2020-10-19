package no.ssb.rawdata.converter.core.datasetmeta;

public enum DatasetType {
    BOUNDED(0),
    UNBOUNDED(1),
    UNRECOGNIZED(-1)
    ;

    private final int value;

    DatasetType(int value) {
        this.value = value;
    }

}
