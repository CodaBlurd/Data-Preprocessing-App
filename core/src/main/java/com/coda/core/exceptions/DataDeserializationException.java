package com.coda.core.exceptions;

import com.coda.core.util.types.ErrorType;
import lombok.Getter;

public class DataDeserializationException extends RuntimeException {

    /**
     * errorType.
     * This attribute is used to store the error type.
     */
    @Getter
    private final ErrorType errorType;

    /**
     * DataDeserializationException() constructor.
     *
     * @param message The message.
     * @param type    The error type.
     * @param e the error thrown.
     */

        public DataDeserializationException(final String message,
                                            final ErrorType type,
                                            final ClassNotFoundException e) {

            super(message);
            this.errorType = type;
        }

    /**
     * DataDeserializationException.
     * @param message the error message.
     * @param type the error type.
     */

    public DataDeserializationException(final String message,
                                            final ErrorType type) {

            super(message);
            this.errorType = type;
        }

}
