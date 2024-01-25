package com.coda.core.exceptions;

import com.coda.core.util.ErrorType;

import java.io.IOException;

public class DataExtractionException extends IOException {
    private ErrorType errorType;

        public DataExtractionException(String message, ErrorType errorType) {
            super(message);
            this.errorType = errorType;
        }
}
