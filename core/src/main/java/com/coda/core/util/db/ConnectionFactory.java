package com.coda.core.util.db;

import com.coda.core.dtos.ConnectionDetails;

import javax.sql.DataSource;

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
     * This method establishes obtain the datasource.
     * @return DataSource object.
     */
    DataSource dataSource();

    void createDataSource(ConnectionDetails connectionDetails);
}

