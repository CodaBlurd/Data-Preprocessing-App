package com.coda.core.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "mysql")
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
}
