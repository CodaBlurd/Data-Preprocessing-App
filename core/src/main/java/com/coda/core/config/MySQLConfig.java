package com.coda.core.config;

import com.coda.core.util.db.SqlDbConnectionFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
public class MySQLConfig {

    /**
     * The SqlDbConnectionFactory object.
     */
    private final SqlDbConnectionFactory sqlDbConnectionFactory;

    /**
     * Constructor to inject dependencies.
     *
     * @param connectionFactory the connection factory
     */

    public MySQLConfig(final SqlDbConnectionFactory connectionFactory) {
        this.sqlDbConnectionFactory = connectionFactory;
    }

    /**
     * Creates a DataSource object.
     *
     * @return DataSource object
     */

    @Bean(name = "dataSource")
    public DataSource mysqlDataSource() {
        DataSource dataSource = sqlDbConnectionFactory.dataSource();
        if (dataSource == null) {
            return defaultDataSource();
        }
        return dataSource;
    }

    /**
     * Creates a DataSource object.
     *
     * @return DataSource object
     */

    @Bean(name = "defaultDataSource")
    public DataSource defaultDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        config.setUsername("sa");
        config.setPassword("");
        config.setDriverClassName("org.h2.Driver");
        return new HikariDataSource(config);
    }

    /**
     * Creates a JdbcTemplate object.
     *
     * @param dataSource the DataSource object
     * @return JdbcTemplate object
     */

    @Bean(name = "jdbcTemplate")
    public JdbcTemplate mysqlJdbcTemplate(@Qualifier("dataSource") final DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * Creates a DataSourceTransactionManager object.
     *
     * @param dataSource the DataSource object
     * @return DataSourceTransactionManager object
     */

    @Bean(name = "mysqlTransactionManager")
    public DataSourceTransactionManager mysqlTransactionManager(@Qualifier("dataSource") final DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}
