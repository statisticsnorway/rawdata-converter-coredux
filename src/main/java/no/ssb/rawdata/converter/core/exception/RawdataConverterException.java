package no.ssb.rawdata.converter.core.exception;

/**
 * Base for exceptions thrown by rawdata converter components
 */
public class RawdataConverterException extends RuntimeException {

    public RawdataConverterException(String message) {
        super(message);
    }

    public RawdataConverterException(String message, Throwable cause) {
        super(message, cause);
    }

}
