package com.coda.core.util.db;

import java.sql.Connection;

/**
 <p>ConnectionFactory is an interface that
 contains all the methods
 that can be used to connect to a database.
 </p>
 * @see ConnectionFactory
 * @see SqlDbConnectionFactory
 */

public interface ConnectionFactory {

    /**
     <p>
     This method is a factory method
     establishes connection to a any sql
     db depending on the implementing class implementation.
     </p>
     * @param url the connection url.
     * @param user the user connecting to the db.
     * @param password the password of the user.
     * @return Connection object.
     * @throws Exception if the connection fails.
     */
    Connection connect(
            String url, String user,
            String password)
            throws Exception;

    /**
     * This method establishes a connection to a mysql db.
     * @return Connection object.
     * @throws Exception if the connection fails.
     */
    Connection connectToMySQL() throws Exception;
}

