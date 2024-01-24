package com.coda.core.util;

import java.sql.Connection;

/**
 * ConnectionFactory is an interface that contains all the methods that can be used to connect to a database.
 * @see ConnectionFactory
 * @see com.coda.core.util.SqlDbConnectionFactory
 *
 */

public interface ConnectionFactory {


    Connection connect(String url, String user, String password) throws Exception;
}
