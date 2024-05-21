package com.coda.core.exceptions;

import com.coda.core.util.types.ErrorType;
import lombok.Getter;

@Getter
public class ValidationException extends RuntimeException {
    /**
     * The error type.
     * used to identify the error thrown.
     */
    private final ErrorType errorType;

    /**
     * Constructor.
     * for the ValidationException
     * class.
     * @param message the error message.
     * @param type the type of the error.
     */
    public ValidationException(final String message, final ErrorType type) {
        super(message);
        this.errorType = type;
    }
}

