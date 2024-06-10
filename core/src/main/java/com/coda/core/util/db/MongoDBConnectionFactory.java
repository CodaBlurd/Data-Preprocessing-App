package com.coda.core.util.db;

import com.coda.core.config.MongoDBProperties;
import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.util.types.ErrorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * <p>
 * MongoDBConnectionFactory is a class that contains
 * all the methods that can be used to
 * establish a connection to a MongoDB database.
 * </p>
 */
@Component
public final class MongoDBConnectionFactory {

    /**
     * The MongoClient object used to connect to the database.
     */
    private MongoClient mongoClient;

    /**
     * The MongoDBProperties object used to configure the connection.
     * The MongoDBProperties object contains the database name,
     * the host name, and url
     * of the database.
     * @see MongoDBProperties
     */

    private final MongoDBProperties mongoDBProperties;

    /**
     * <p>This constructor is used to
     * create a connection to the MongoDB database.
     * </p>
     * @param properties The MongoDBProperties bean.
     * @param client The MongoClient bean.
     */
    @Autowired
    public MongoDBConnectionFactory(final MongoDBProperties properties,
                                    final MongoClient client) {
        this.mongoDBProperties = properties;
        this.mongoClient = client;
    }

    /**
     * This method is used to create a connection to the MongoDB database.
     * @param client The MongoClient object
     * @param dbName The name of the database
     * @return A connection to the database
     * @throws ReadFromDbExceptions if the connection fails
     */
    public MongoDatabase getConnection(final MongoClient client,
                                       final String dbName) {

        if (client == null) {
            throw new ReadFromDbExceptions("Failed to connect "
                    + "to database", ErrorType.DB_NOT_FOUND);
        } else if (dbName == null || dbName.isEmpty()) {
            throw new IllegalArgumentException("Invalid database name");
        }

        return client.getDatabase(dbName);
    }

    /**
     * This method is used to close
     * the connection to the MongoDB database.
     */
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }
}
