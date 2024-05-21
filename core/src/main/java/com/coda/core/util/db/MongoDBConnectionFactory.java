package com.coda.core.util.db;

import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.util.types.ErrorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 <p>
    MongoDBConnectionFactory is a class that contains
    all the methods that can be used to
    establish a connection to a MongoDB database.
 </p>
 */

@Component
public final class MongoDBConnectionFactory {

    /**
     * The MongoClient object used to connect to the database.
     */
    private final MongoClient mongoClient;

    /**
     <p>This constructor is used to
     create a connection to the MongoDB database.
     </p>
     * @param connectionUrl The url of the database connection.
     * @throws ReadFromDbExceptions if the connection fails
     * or the url is invalid.
     */

    public MongoDBConnectionFactory(@Value("${spring.data.mongodb.uri}")
                                    final String connectionUrl) {
        if (connectionUrl == null
                || connectionUrl.isEmpty()) {

            throw new ReadFromDbExceptions(
                    "Invalid connection url",
                    ErrorType.DB_NOT_FOUND);
        }

        this.mongoClient
                = MongoClients.create(connectionUrl);
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
     * This method is used to close
     * the connection to the MongoDB database.
     */
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

}
