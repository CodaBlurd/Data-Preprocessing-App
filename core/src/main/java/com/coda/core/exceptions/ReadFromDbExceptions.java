package com.coda.core.exceptions;

import com.coda.core.util.ErrorType;
import lombok.Getter;

/**
 * Custom exception class for ReadFromDbExceptions
 * <p> This class is responsible for all the custom exceptions related to reading from the database</p>
 */



public class ReadFromDbExceptions extends RuntimeException{

    @Getter
    private final ErrorType errorType;
    public ReadFromDbExceptions(String message, ErrorType errorType) {
        super(message);
        this.errorType = errorType;
    }




}
