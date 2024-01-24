package com.coda.core.util;

/**
 * This interface is used to define the queries
 * <p> This interface is used to define the queries for the application </p>
 * The queries are defined as constants
 */

public interface QueriesInterface {

    // Query to extract data from mySql
    String READ_FROM_MYSQL = "SELECT * FROM %s";

    // Query to extract data from mongoDb
    String READ_FROM_MONGO_DB = "db.%s.find({})";
}
