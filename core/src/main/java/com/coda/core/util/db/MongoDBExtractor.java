package com.coda.core.util.db;

import com.coda.core.config.MongoDBConfig;
import com.coda.core.config.MongoDBProperties;
import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.util.timestamps.FileTimestampStorage;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 <p> MongoDBExtractor is a class that contains
 all the methods that can be used to
 extract data from a MongoDB database.
 </p>
 * @see MongoDBExtractor
 */

@Component
public final class MongoDBExtractor implements DatabaseExtractor {

    /**
     * MongoDBConnectionFactory is a custom class that
     * provides methods to create a connection
     * to a MongoDB database.
     * used as  a dependency in this class.
     * @see MongoDBConnectionFactory
     */
    private final MongoDBConnectionFactory mongoDBConnectionFactory;

    /**
     * The MongoConfig object.
     */
    private final MongoDBConfig mongoDBConfig;

    /**
     * The FileTimestampStorage object.
     */
    private final FileTimestampStorage fileTimestampStorage;

    /**
     * This constructor initializes the mongoDBConnectionFactory field
     * with the value provided.
     * @param mongoConnectFactory The MongoDBConnectionFactory object
     * @param config The MongoDBConfig object
     * @param fts The FileTimestampStorage object
     * @see MongoDBConnectionFactory
     * @see MongoDBConfig
     */

    @Autowired
    public MongoDBExtractor(
            @Lazy final MongoDBConnectionFactory mongoConnectFactory,
            final MongoDBConfig config, final FileTimestampStorage fts) {
        this.mongoDBConnectionFactory = mongoConnectFactory;
        this.mongoDBConfig = config;
        this.fileTimestampStorage = fts;
    }

    /**
     <p>This method returns a list of DataModel
     objects that contain the data from the table provided.
     </p>
     * @param tableName The name of the table to be extracted.
     * @param url The url of the database.
     * @return List<DataModel<Object>> A list of DataModel objects.
     */
    @Override
    public Map<String, DataModel<Document>> readData(
            final String databaseName,
            final String tableName, final String url) {
        if (databaseName == null || tableName == null || url == null) {
            throw new IllegalArgumentException("Invalid arguments");
        } else if (databaseName.isEmpty()
                || tableName.isEmpty() || url.isEmpty()) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        MongoClient mongoClient = null;

        try {

            mongoClient = mongoDBConfig.mongoClient();

            MongoDBProperties tempProperties = new MongoDBProperties();
            tempProperties.setUrl(url);

            MongoDatabase mongoDatabase
                    = mongoDBConnectionFactory
                    .getConnection(mongoClient, databaseName);

            MongoCollection<Document> collection
                    = mongoDatabase.getCollection(tableName);

            Map<String, DataModel<Document>> dataModelList = new HashMap<>();

            Instant lastExtractedTimeStamp = fileTimestampStorage
                    .getLastExtractedTimestamp();

            // Filters.gt

            Bson filter = Filters.gt("updatedAt", lastExtractedTimeStamp);

            for (Document document : collection.find(filter)) {
                DataModel<Document> dataModel = new DataModel<>();
                Map<String, DataAttributes<Document>> attributes
                        = new HashMap<>();
                for (String key : document.keySet()) {
                    DataAttributes<Document> attribute
                            = new DataAttributes<>(
                            key, document.get(key),
                            "Document",
                            Document.class);
                    attributes.put(key, attribute);
                }
                dataModel.setAttributesMap(attributes);
                dataModelList.put(document.get("_id").toString(), dataModel);
            }
            fileTimestampStorage.updateLastExtractedTimestamp(Instant.now());
            return dataModelList;
        } finally {
            if (mongoClient != null) {
                mongoDBConnectionFactory.close();
            }
        }
    }

    /*
    <p>This method is used to extract data from a MongoDB database.
    not required by this class.</p>
    */
    @Override
    public List<DataModel<Object>> readData(
            final String tableName) {
        return Collections.emptyList();
    }
}
