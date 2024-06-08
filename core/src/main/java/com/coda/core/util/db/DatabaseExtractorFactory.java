package com.coda.core.util.db;

import org.springframework.beans.factory.annotation.Autowired;
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
     * The SQL connection factory.
     */

    private final ConnectionFactory connectionFactory;

    /**
     * The MySqlExtractor.
     */

    private final MySQLExtractor mySQLExtractor;


    /**
     * The MongoDbExtractor.
     */
    private final MongoDBExtractor mongoDBExtractor;

    /**
     * The constructor.
     * @param connection The connection factory.
     * @param sqlExtractor The mysql extractor.
     * @param noSqlExtractor The mongodb extractor.
     * @see MySQLExtractor
     * @see MongoDBExtractor
     * @see ConnectionFactory
     */

    @Autowired
    public DatabaseExtractorFactory(final ConnectionFactory connection,
                                    final MySQLExtractor sqlExtractor,
                                    final MongoDBExtractor noSqlExtractor) {
        this.connectionFactory = connection;
        this.mySQLExtractor = sqlExtractor;
        this.mongoDBExtractor = noSqlExtractor;
    }

    /**
     * This method returns the appropriate
     * DatabaseExtractor based on
     * the database type provided.
     * @param databaseType The type of the database
     * @return DatabaseExtractor
     */

    public DatabaseExtractor getExtractor(final String databaseType) {
        return switch (databaseType) {
            case DatabaseNames.MYSQL -> {
                mySQLExtractor
                        .setConnectionFactory(connectionFactory);
                yield mySQLExtractor;
            }
            case DatabaseNames.MONGODB -> mongoDBExtractor;
            // other database types will be added here
            default -> throw new IllegalArgumentException(
                    "Unsupported database type: " + databaseType);
        };
    }
}
