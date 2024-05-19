package com.coda.core.util.db;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 <p>
        SqlDbConnectionFactory is a class that
        creates a connection to a SQL database.
 </p>
    <p>
            It implements the ConnectionFactory interface
            and overrides the connect method to create
            a connection to a SQL database.
    </p>
 */

@Slf4j
public class SqlDbConnectionFactory implements ConnectionFactory {

    // Load the MySQL driver class
        static {
            try {
                Class.forName(DatabaseNames.MYSQL_DRIVER_MANAGER);
            } catch (ClassNotFoundException e) {
                log.error(
                        "Error loading MySQL driver class, "
                                + "Message: {}, Cause: {}",
                        e.getMessage(), e.getCause(), e);

            }
        }

        /**
         <p>
                This method is used to
         create a connection to a SQL database.
         </p>
         * @param url The url of the database
         * @param user The username of the database
         * @param password The password of the database
         * @return A connection to the database
         * @throws Exception if the connection fails
         */
        @Override
        public Connection connect(final String url, final String user,
                                  final String password) throws Exception {
            return DriverManager.getConnection(url, user, password);

        }
    }
