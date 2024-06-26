package com.coda.core.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "spring.datasource")
@Getter @Setter
public class MySQLProperties {

    /**
     * The url of the database.
     */
    private String url;

    /**
     * The username of the database.
     */
    private String username;

    /**
     * The password of the database.
     */
    private String password;

    /**
     * The driver class name of the database.
     */

    private String driverClassName;

    // HikariCP settings

    /**
     * The maximum number of connections that can be
     * allocated in the connection pool.
     */
    private int maximumPoolSize;

    /**
     * The idle timeout in milliseconds for connections in the pool.
     */
    private long idleTimeout;

    /**
     * The maximum lifetime in milliseconds of a connection in the pool.
     */
    private long maxLifetime;

    /**
     * The connection timeout in milliseconds for the connection.
     */
    private long connectionTimeout;
}
