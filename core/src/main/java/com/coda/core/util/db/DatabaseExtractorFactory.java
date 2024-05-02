package com.coda.core.util.db;

import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

/**
 * DatabaseExtractorFactory is a factory class that returns the appropriate DatabaseExtractor
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
     * This method returns the appropriate DatabaseExtractor based on the database type provided.
     * @param databaseType The type of the database
     * @return DatabaseExtractor
     */

    public DatabaseExtractor getExtractor(String databaseType) {
        switch (databaseType) {
            case DatabaseNames.MYSQL:
                return new MySQLExtractor();
            case DatabaseNames.MONGODB:
                return new MongoDBExtractor();
            // other database types will be added here
            default:
                throw new IllegalArgumentException("Unsupported database type: " + databaseType);
        }
    }
}
