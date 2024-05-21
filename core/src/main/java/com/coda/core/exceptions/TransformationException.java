package com.coda.core.exceptions;

import com.coda.core.util.types.ErrorType;
import lombok.Getter;

@Getter
public class TransformationException extends RuntimeException {

    /**
     * The type of the error.
     */
    private final ErrorType errorType;

    /**
     * Constructor,
     * for the TransformationException class.
     * @param message the error message
     * @param type the type of the error
     */

    public TransformationException(final String message,
                                   final ErrorType type) {


        super(message);
        this.errorType = type;

    }
}

