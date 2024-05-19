package com.coda.core.exceptions;

import com.coda.core.util.types.ErrorType;

import java.io.IOException;

public final class DataExtractionException
        extends IOException {

    /**
     * The error type of the exception.
     */
    private final ErrorType errorType;

    /**
     * Constructor for DataExtractionException.
     * @param message the message to display.
     * @param type the error type of the exception.
     */

        public DataExtractionException(final String message,
                                       final ErrorType type) {
            super(message);
            this.errorType = type;
        }
}
