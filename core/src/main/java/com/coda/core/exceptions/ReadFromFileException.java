package com.coda.core.exceptions;

import com.coda.core.util.ErrorType;
import lombok.Getter;

@Getter
public class ReadFromFileException extends RuntimeException{
    private final ErrorType errorType;
    public ReadFromFileException(String message, ErrorType errorType) {
        super(message);
        this.errorType = errorType;
    }
}
