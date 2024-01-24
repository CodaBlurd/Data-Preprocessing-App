package com.coda.core.util;

/**
 * This class is used to define the error types
 * <p> This class is used to define the error types for the exceptions thrown in the application</p>
 * The error types are defined as enum constants
 */
public enum ErrorType {

    //== constants ==

    //== ReadFromDbExceptions ==
    DB_NOT_SUPPORTED,
    INVALID_DB_CREDENTIALS, DB_NOT_FOUND,
    READ_FROM_DB_EXCEPTIONS;
}
