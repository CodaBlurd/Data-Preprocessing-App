package com.coda.core.util.db;

/**
 * Defines the queries for the application.
 * <p>
 * This class is used to define the queries for the application.
 * The queries are defined as constants.
 * </p>
 */
public final class Queries {

    private Queries() {
        // private constructor to prevent instantiation
    }

    /**
     * Query to extract data from MySQL.
     * <p>
     * The query is defined as a constant.
     * </p>
     */
    public static final String READ_FROM_MYSQL
            = "SELECT * FROM %s LIMIT ? OFFSET ?";

    /**
     * Query to extract data from MongoDB.
     * <p>
     * The query is defined as a constant.
     * </p>
     */
    public static final String READ_FROM_MONGO_DB = "db.%s.find({})";
}
