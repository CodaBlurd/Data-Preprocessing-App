package com.coda.core.util;

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
    public static final String ORACLE = "oracle";
    public static final String SQLSERVER = "sqlserver";
    public static final String HIVE = "hive";
    public static final String HBASE = "hbase";
    public static final String CASSANDRA = "cassandra";
    public static final String REDIS = "redis";
    public static final String NEO4J = "neo4j";
    public static final String ELASTICSEARCH = "elasticsearch";
    public static final String COUCHBASE = "couchbase";
    public static final String MARIADB = "mariadb";
    public static final String DB2 = "db2";
    public static final String SYBASE = "sybase";
    public static final String TERADATA = "teradata";
    public static final String INFORMIX = "informix";
    public static final String SNOWFLAKE = "snowflake";
    public static final String BIGQUERY = "bigquery";
    public static final String FIREBASE = "firebase";
    public static final String AMAZON_REDSHIFT = "amazon_redshift";
    public static final String AMAZON_AURORA = "amazon_aurora";
    public static final String AMAZON_DYNAMODB = "amazon_dynamodb";
}
