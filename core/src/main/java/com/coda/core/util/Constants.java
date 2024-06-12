package com.coda.core.util;

/**
 * This class contains all the constants used in the application.
 */
public final class Constants {

    /**
     * constructor().
     */
    private Constants() { }

    /**
     * enum DataSourceType.
     */
    public enum DataSourceType {

        /**
         * Enum values.
         */
        FILE_SYSTEM, CLASSPATH, SQL, MONGO
    }

    /**
     * BATCH_SIZE.
     */
    public static final int BATCH_SIZE = 100;
}
