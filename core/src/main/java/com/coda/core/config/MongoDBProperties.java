package com.coda.core.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "mongodb")
@Getter @Setter
public class MongoDBProperties {

    /**
     * MongoDB connection URL.
     */
    private String url;

    /**
     * MongoDB username.
     */
    private String username;
    /**
     * MongoDB password.
     */
    private String password;
    /**
     * MongoDB database name.
     */
    private String database;



    // getters and setters


}
