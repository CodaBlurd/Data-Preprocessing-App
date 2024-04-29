package com.coda.core.util.db;

/**
 * DatabaseNames is a class that contains all the database and driver manager names that can be used in the application.
 * They are represented as constants and used to identify the database type.
 * @see com.coda.core.service.DataModelService
 * */

public class DatabaseNames {

    // == Driver Manager constants ==
    // Driver manager constants for different databases
    public static final String MYSQL_DRIVER_MANAGER = "com.mysql.cj.jdbc.Driver";
    public static final String POSTGRESQL_DRIVER_MANAGER = "org.postgresql.Driver";


    public static final String MYSQL = "mysql";
    public static final String MONGODB = "mongodb";
    public static final String POSTGRESQL = "postgresql";
}
