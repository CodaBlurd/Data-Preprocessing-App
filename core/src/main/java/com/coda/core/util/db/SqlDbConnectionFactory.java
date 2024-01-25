package com.coda.core.util.db;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * SqlDbConnectionFactory is a factory class that is used to create a connection to a SQL database.
 * <p> This class is responsible for creating a connection to a SQL database</p>
 * */

public class SqlDbConnectionFactory implements ConnectionFactory{


        // load the driver class using static block
        // static block is executed first when the class is loaded
        static {
            try {
                Class.forName(DatabaseNames.MYSQL_DRIVER_MANAGER);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        /**
         * This method is used to create a connection to a SQL database
         * @param url The url of the database
         * @param user The username of the database
         * @param password The password of the database
         * @return A connection to the database
         * @throws Exception if the connection fails
         */
        @Override
        public Connection connect(String url, String user, String password) throws Exception {
            return DriverManager.getConnection(url, user, password);

        }
    }
