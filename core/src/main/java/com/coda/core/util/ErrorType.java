package com.coda.core.util;

/**
 * This class is used to define the error types
 * <p> This class is used to define the error types for the exceptions thrown in the application</p>
 * The error types are defined as enum constants
 */
public enum ErrorType {

    //== constants ==
    DB_NOT_SUPPORTED,
    INVALID_DB_CREDENTIALS, DB_NOT_FOUND,
    READ_FROM_DB_EXCEPTIONS,

    //== files errors ==
    FILE_NOT_FOUND,
    FILE_NOT_READABLE,
    FILE_NOT_WRITABLE,
    PERMISSION_DENIED,
    FILE_ALREADY_EXISTS, VALIDATION_FAILED,
}
