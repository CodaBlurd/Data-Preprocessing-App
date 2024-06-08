package com.coda.core.util.db;

import com.coda.core.config.MySQLProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
@Component
public class SqlDbConnectionFactory implements ConnectionFactory {

    /**
     * This field stores the MySQLProperties object.
     * The MySqlProperties class is used to store
     * the database connection details.
     * @see MySQLProperties
     */
    private final MySQLProperties mySQLProperties;

    /**
     * This constructor initializes the mySQLProperties field
     * with the value provided.
     * @param properties The MySQLProperties object
     */
    @Autowired
    public SqlDbConnectionFactory(final MySQLProperties properties) {
        this.mySQLProperties = properties;
    }

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

    /**
     * This method is used to create a connection to a MySQL database.
     * connectToMySQL is a method that creates a connection
     *  to a MySQL database.
     * @return A connection to the MySQL database
     * @throws Exception if the connection fails
     */

    @Override
    public Connection connectToMySQL() throws Exception {
        return connect(mySQLProperties.getUrl(),
                mySQLProperties.getUsername(),
                mySQLProperties.getPassword());
    }
}
