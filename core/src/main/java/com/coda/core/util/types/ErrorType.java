package com.coda.core.util.types;

/**
 * This class is used to define the error types.
 * <p>
 * The error types are used
 * to identify the type of error
 * </p>
 */
public enum ErrorType {

    /**
     * DB_NOT_SUPPORTED
     * <p>
     * DATABASE_NOT_SUPPORTED is used to identify
     * when the database is not supported.
     * </p>
     */
    DB_NOT_SUPPORTED,

    /**
     * INVALID_DB_CREDENTIALS
     * <p>
     * INVALID_DB_CREDENTIALS is used to identify
     * when the database credentials are invalid.
     * </p>
     */
    INVALID_DB_CREDENTIALS, DB_NOT_FOUND,

    /**
     * DB_CONNECTION_FAILED
     * <p>
     * DB_CONNECTION_FAILED is used to identify
     * when the database connection fails.
     * </p>
     */
    READ_FROM_DB_EXCEPTIONS,



    /**
     * FILE_NOT_FOUND
     * <p>
     * FILE_NOT_FOUND is used to identify
     * when the file is not found.
     * </p>
     */
    FILE_NOT_FOUND,

    /**
     * FILE_NOT_READABLE
     * <p>
     * FILE_NOT_READABLE is used to identify
     * when the file is not readable.
     * </p>
     */
    FILE_NOT_READABLE,

    /**
     * FILE_NOT_WRITABLE
     * <p>
     * FILE_NOT_WRITABLE is used to identify
     * when the file is not writable.
     * </p>
     */
    FILE_NOT_WRITABLE,

    /**
     * PERMISSION_DENIED
     * <p>
     * PERMISSION_DENIED is used to identify
     * when the permission is denied.
     * </p>
     */
    PERMISSION_DENIED,

    /**
     * FILE_ALREADY_EXISTS
     * <p>
     * FILE_ALREADY_EXISTS is used to identify
     * when the file already exists.
     * </p>
     */
    FILE_ALREADY_EXISTS,
    /** VALIDATION_FAILED
     * <p>
     * VALIDATION_FAILED is used to identify
     * when the validation fails based on set of validation rules .
     * </p>
     */

    VALIDATION_FAILED,

    /**
     * DATA_EXTRACTION_FAILED
     * <p>
     * DATA_EXTRACTION_FAILED is used to identify
     * when the data extraction fails.
     * </p>
     */
    DATA_EXTRACTION_FAILED,

    /**
     * NO_DATA
     * <p>
     * NO_DATA is used to identify
     * when there is no data.
     * </p>
     */
    NO_DATA,

    /**
     * READ_ERROR is used to identify
     * an error thrown
     * from reading data from data sources.
     */
    READ_ERROR,

    /**
     * TRANSFORMATION_FAILED
     * this error constant is used to
     * identify when transformation
     * of the data value or values failed.
     */

    TRANSFORMATION_FAILED,

    /**
     * TRANSFORMATION_STRATEGY_NOT_FOUND
     * identifies error thrown when no,
     * suitable transformation type found.
     */
    TRANSFORMATION_STRATEGY_NOT_FOUND,

    /**
     * DATA_DESERIALIZATION_ERROR.
     * Identify when serialization
     * of class data failed.
     */
    DATA_DESERIALIZATION_ERROR,

    /**
     * ACCESS_DENIED.
     * Identify when access denied
     * to the data.
     */
    ACCESS_DENIED,

    /**
     * INVALID_FILE_PATH.
     * Identify when file path is invalid.
     */
    INVALID_FILE_PATH,

    /**
     * UNKNOWN.
     * Identify when error is unknown.
     */
    UNKNOWN,

    /**
     * DATA_SAVE_ERROR.
     * Identify when data save failed.
     */
    DATA_SAVE_ERROR,

    /**
     * UNKNOWN_ATTRIBUTE_TYPE.
     * Identify when attribute type is unknown.
     */
    UNKNOWN_ATTRIBUTE_TYPE,
}

