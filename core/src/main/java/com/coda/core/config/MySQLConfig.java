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
     * SQL database connection factory.
     */
    private final SqlDbConnectionFactory sqlDbConnectionFactory;

    /**
     * Constructor to inject dependencies.
     * @param connectionFactory the SQL database connection factory
     */
    public MySQLConfig(final SqlDbConnectionFactory connectionFactory) {
        this.sqlDbConnectionFactory = connectionFactory;
    }

    /**
     * MySQL data source.
     * @return the MySQL data source
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
     * Default data source.
     * @return the default data source
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
     * MySQL JDBC template.
     * @param dataSource the data source
     * @return the MySQL JDBC template
     */
    @Bean(name = "jdbcTemplate")
    public JdbcTemplate mysqlJdbcTemplate(
            @Qualifier("dataSource") final DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * MySQL transaction manager.
     * @param dataSource the data source
     * @return the MySQL transaction manager
     */

    @Bean(name = "mysqlTransactionManager")
    public DataSourceTransactionManager mysqlTransactionManager(
            @Qualifier("dataSource") final DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}
