package com.coda.core.exceptions;

import com.coda.core.util.types.ErrorType;
import lombok.Getter;

/**
 * Custom exception class for ReadFromDbExceptions.
 * <p> This class is
 * responsible for all the custom exceptions
 * related to reading from the database</p>
 */


@Getter
public final class ReadFromDbExceptions
        extends RuntimeException {

    /**
     * The error type of the exception.

     */
    private final ErrorType errorType;

    /**
     * Constructor for ReadFromDbExceptions.
     * @param message the message to display.
     * @param type the error type of the exception.
     */
    public ReadFromDbExceptions(
            final String message,
            final ErrorType type) {
        super(message);
        this.errorType = type;
    }

}

