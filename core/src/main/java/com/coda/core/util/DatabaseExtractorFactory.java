package com.coda.core.util;

/**
 * DatabaseExtractorFactory is a factory class that returns the appropriate DatabaseExtractor
 * based on the database type provided.
 * @see DatabaseExtractorFactory
 * @see DatabaseExtractor
 * @see MySQLExtractor
 * @see DatabaseNames
 * @see DatabaseNames#MYSQL
 */
public class DatabaseExtractorFactory {

    /**
     * This method returns the appropriate DatabaseExtractor based on the database type provided.
     * @param databaseType
     * @return DatabaseExtractor
     */

    public DatabaseExtractor getExtractor(String databaseType) {
        switch (databaseType) {
            case DatabaseNames.MYSQL:
                return new MySQLExtractor();
            // ... other cases
            default:
                throw new IllegalArgumentException("Unsupported database type: " + databaseType);
        }
    }
}
