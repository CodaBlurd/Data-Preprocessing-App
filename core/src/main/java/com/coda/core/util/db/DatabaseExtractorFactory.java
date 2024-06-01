package com.coda.core.util.db;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * DatabaseExtractorFactory is a
 * factory class that returns
 * the appropriate DatabaseExtractor
 * based on the database type provided.
 * @see DatabaseExtractorFactory
 * @see DatabaseExtractor
 * @see MySQLExtractor
 * @see DatabaseNames
 * @see DatabaseNames#MYSQL
 */
@Component
public class DatabaseExtractorFactory {

    /**
     * The application context.
     */
    private final ApplicationContext applicationContext;

    /**
     * The constructor.
     * @param appContext The application context.
     */

    @Autowired
    public DatabaseExtractorFactory(final ApplicationContext appContext) {
        this.applicationContext = appContext;
    }

    /**
     * This method returns the appropriate
     * DatabaseExtractor based on
     * the database type provided.
     * @param databaseType The type of the database
     * @return DatabaseExtractor
     */

    public DatabaseExtractor getExtractor(final String databaseType) {
        switch (databaseType) {
            case DatabaseNames.MYSQL:
                return new MySQLExtractor();
            case DatabaseNames.MONGODB:
                return applicationContext.getBean(MongoDBExtractor.class);
            // other database types will be added here
            default:
                throw new IllegalArgumentException(
                        "Unsupported database type: " + databaseType);
        }
    }
}
