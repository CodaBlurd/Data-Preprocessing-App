package com.coda.core.util.db;

import com.coda.core.config.MySQLProperties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

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
     * This field stores the HikariDataSource object.
     * The HikariDataSource class is used to create
     * a connection pool to the database.
     * @see HikariDataSource
     */
    private HikariDataSource dataSource;

    /**
     * This constructor initializes the mySQLProperties field
     * with the value provided.
     * @param properties MySQLProperties object
     */
    @Autowired
    public SqlDbConnectionFactory(final MySQLProperties properties) {
        this.mySQLProperties = properties;
        this.dataSource = initializeDataSource();

    }

    /**
     * This method returns the DataSource object.
     * @return DataSource object
     */

    @Override
    public DataSource dataSource() {
        return this.dataSource;
    }

    /**
     * This method initializes the HikariDataSource object.
     * with the database connection details.
     *
     * @see HikariDataSource
     * @see MySQLProperties
     * @see DatabaseNames
     * @return HikariDataSource object
     */
    private HikariDataSource initializeDataSource() {
        log.info("Initializing Hikari DataSource");

        if (mySQLProperties.getUrl() == null
                || mySQLProperties.getUrl().isEmpty()
                || mySQLProperties.getUsername() == null
                || mySQLProperties.getPassword().isEmpty()
                || mySQLProperties.getDriverClassName() == null
                || mySQLProperties.getDriverClassName().isEmpty()) {
            log.warn("MySQL properties are not fully set,"
                    + " skipping DataSource initialization");
            return null;
        }

        HikariConfig config = getHikariConfig();

        return new HikariDataSource(config);

    }

    private HikariConfig getHikariConfig() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(mySQLProperties.getUrl());
        config.setUsername(mySQLProperties.getUsername());
        config.setPassword(mySQLProperties.getPassword());
        config.setDriverClassName(mySQLProperties.getDriverClassName());

        config.setMaximumPoolSize(mySQLProperties.getMaximumPoolSize());
        config.setMaxLifetime(mySQLProperties.getMaxLifetime());
        config.setConnectionTimeout(mySQLProperties.getConnectionTimeout());
        config.setIdleTimeout(mySQLProperties.getIdleTimeout());
        config.setAutoCommit(true);
        config.addDataSourceProperty("cachePrepStmts", true);
        return config;
    }
}
