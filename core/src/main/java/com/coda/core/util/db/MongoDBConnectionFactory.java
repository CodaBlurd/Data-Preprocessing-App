package com.coda.core.util.db;

import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.util.ErrorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

/**
 * This connection factory class is used to create a connection to the MongoDB database
 * <p> This class is responsible for creating a connection to the MongoDB database</p>

 */

public class MongoDBConnectionFactory {

    private MongoClient mongoClient;

    /**
     * This method is used to establish a  mongoDB connection
     * @param connectionUrl The url of the database
     * @throws ReadFromDbExceptions if the connection fails
     */

    public MongoDBConnectionFactory(String connectionUrl) {
        if (connectionUrl == null || connectionUrl.isEmpty()) {
            throw new ReadFromDbExceptions("Invalid connection url",
                    ErrorType.DB_NOT_FOUND);
        }
        this.mongoClient = MongoClients.create(connectionUrl);
    }

    /**
     * This method is used to create a connection to the MongoDB database
     * @param dbName The name of the database
     * @return A connection to the database
     * @throws ReadFromDbExceptions if the connection fails
     */

    public MongoDatabase getConnection(String dbName) {
        if (mongoClient == null) {
            throw new ReadFromDbExceptions("Failed to connect to database",
                    ErrorType.DB_NOT_FOUND);
        } else if (dbName == null || dbName.isEmpty()) {
           throw new IllegalArgumentException("Invalid database name");
        }
        return mongoClient.getDatabase(dbName);
    }

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}