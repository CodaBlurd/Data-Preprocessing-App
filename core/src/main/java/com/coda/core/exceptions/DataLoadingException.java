package com.coda.core.exceptions;

import com.coda.core.util.types.ErrorType;
import lombok.Getter;

@Getter
public class DataLoadingException extends RuntimeException {
    /**
     * The errorType thrown.
     */
    private final ErrorType errorType;

    /**
     * Constructor.
     * @param msg the error message.
     * @param type  the error type.
     */

    public DataLoadingException(final String msg, final ErrorType type) {
        super(msg);
        this.errorType = type;
    }
}
