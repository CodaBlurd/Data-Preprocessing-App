package com.coda.core.util.db;

import com.coda.core.dtos.ConnectionDetails;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

/**
 * SqlDbConnectionFactory is a class that creates a connection to a SQL database.
 */
@Slf4j
@Component
public class SqlDbConnectionFactory implements ConnectionFactory {

    private HikariDataSource dataSource;

    /**
     * The driver class name of the database.
     */
    private final static String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";

    /**
     * The maximum number of connections that can be
     * allocated in the connection pool.
     */
    private final static int MAXIMUM_POOL_SIZE = 10;
    /**
     * The idle timeout in milliseconds for connections in the pool.
     */
    private final static long IDLE_TIME_OUT = 600000;
    /**
     * The maximum lifetime in milliseconds of a connection in the pool.
     */
    private final static long MAX_LIFE_TIME = 600000;
    /**
     * The connection timeout in milliseconds for the connection.
     */
    private final static long CONNECTION_TIME_OUT = 30000;

    /**
     * Dynamically creates a DataSource based on user-provided connection details.
     * @param connectionDetails The connection details provided by the user.
     */

    @Override
    public void createDataSource(ConnectionDetails connectionDetails) {
        log.info("Initializing Hikari DataSource with user-provided connection details");

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(connectionDetails.getUrl());
        config.setUsername(connectionDetails.getUsername());
        config.setPassword(connectionDetails.getPassword());
        config.setDriverClassName(DRIVER_CLASS_NAME);

        config.setMaximumPoolSize(MAXIMUM_POOL_SIZE);
        config.setMaxLifetime(MAX_LIFE_TIME);
        config.setConnectionTimeout(CONNECTION_TIME_OUT);
        config.setIdleTimeout(IDLE_TIME_OUT);
        config.setAutoCommit(true);
        config.addDataSourceProperty("cachePrepStmts", true);

        this.dataSource = new HikariDataSource(config);
    }

    /**
     * Returns the current DataSource.
     * @return DataSource object
     */
    @Override
    public DataSource dataSource() {
        return this.dataSource;
    }
}
