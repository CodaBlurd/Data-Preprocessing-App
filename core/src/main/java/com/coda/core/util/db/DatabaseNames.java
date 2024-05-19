package com.coda.core.util.db;

/**
 <p>DatabaseNames is a class that contains all
 * the database and driver manager names
 * that can be used in the application.
 </p>
 <p>They are represented as constants and
  used to identify the database type.
 </p>
 * @see com.coda.core.service.DataModelService
 * */

public final class DatabaseNames {

    // private constructor to prevent instantiation
    private DatabaseNames() { }

    /**
     <p>
        The driver manager for MySQL.
     </p>
     */

    public static final String MYSQL_DRIVER_MANAGER
            = "com.mysql.cj.jdbc.Driver";

    /**
     <p>
        The driver manager for MongoDB.
     </p>
     */
    public static final String POSTGRESQL_DRIVER_MANAGER
            = "org.postgresql.Driver";

    /**
     <p>
     Constant representing the MySQL database.
     </p>
     */
    public static final String MYSQL = "mysql";

    /**
     <p>
     Constant representing the MongoDB database.
     </p>
     */
    public static final String MONGODB = "mongodb";

    /**
     <p>
     Constant representing the POSTGRESQL database.
     </p>
     */
    public static final String POSTGRESQL = "postgresql";
}
