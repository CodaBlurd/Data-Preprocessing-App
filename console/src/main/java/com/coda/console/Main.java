package com.coda.console;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@SpringBootApplication(scanBasePackages = {"com.coda.core", "com.coda.console"})
@EnableMongoRepositories(basePackages = "com.coda.core.repository")
@EnableMongoAuditing
public class Main {

    /**
     * Main method to start the application.
     * @param args command line arguments
     */
    public static void main(final String[] args) {
        SpringApplication.run(Main.class, args);
    }
}
