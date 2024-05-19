package com.coda.core.exceptions;

import com.coda.core.util.types.ErrorType;
import lombok.Getter;

@Getter
public class ReadFromFileException extends RuntimeException {

    /**
     * The error type of the exception.
     */
    private final ErrorType errorType;

    /**
     * Constructor for ReadFromFileException.
     * @param message the message to display.
     * @param type the error type of the exception.
     */
    public ReadFromFileException(
            final String message,
            final ErrorType type) {
        super(message);
        this.errorType = type;
    }
}
