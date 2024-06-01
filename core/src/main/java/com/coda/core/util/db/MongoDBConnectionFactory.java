package com.coda.core.util.db;

import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.util.types.ErrorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
    private final MongoClient mongoClient;

    /**
     * The connection URL of the database.
     * This is used to connect to the database.
     */

    private final String connectionUrl;

    /**
     * <p>This constructor is used to
     * create a connection to the MongoDB database.
     * </p>
     * @param client The MongoClient bean.
     * @param url The connection URL of the database.
     */
    @Autowired
    public MongoDBConnectionFactory(final MongoClient client,
                                    @Value("${spring.data.mongodb.uri}")
                                    final String url) {
        this.mongoClient = client;
        this.connectionUrl = url;
    }

    /**
     * This method is used to create a connection to the MongoDB database.
     * @param dbName The name of the database
     * @return A connection to the database
     * @throws ReadFromDbExceptions if the connection fails
     */
    public MongoDatabase getConnection(final String dbName) {
        if (mongoClient == null) {
            throw new ReadFromDbExceptions("Failed to connect to database",
                    ErrorType.DB_NOT_FOUND);
        } else if (dbName == null || dbName.isEmpty()) {
            throw new IllegalArgumentException("Invalid database name");
        }
        return mongoClient.getDatabase(dbName);
    }

    /**
     * This method is used to get the MongoClient object.
     * @return The MongoClient object.
     */
    public MongoClient getMongoClient() {
        return mongoClient;
    }

    /**
     * This method is used to get the connection URL.
     * @return The connection URL string.
     */

    public String getConnectionUrl() {
        return connectionUrl;
    }

    /**
     * This method is used to close
     * the connection to the MongoDB database.
     */
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
