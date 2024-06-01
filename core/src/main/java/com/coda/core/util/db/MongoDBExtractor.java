package com.coda.core.util.db;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
     * This constructor initializes the mongoDBConnectionFactory field
     * with the value provided.
     * @param mongoConnectFactory The MongoDBConnectionFactory object
     */

    @Autowired
    public MongoDBExtractor(
            final MongoDBConnectionFactory mongoConnectFactory) {
        this.mongoDBConnectionFactory = mongoConnectFactory;
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

        MongoDatabase mongoDatabase
                = mongoDBConnectionFactory.getConnection(databaseName);
        MongoCollection<Document> collection
                = mongoDatabase.getCollection(tableName);

        Map<String, DataModel<Document>> dataModelList = new HashMap<>();

        for (Document document : collection.find()) {
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

        mongoDBConnectionFactory.close();
        return dataModelList;
    }

    /*
    <p>This method is used to extract data from a MongoDB database.
    not required by this class.</p>
    */
    @Override
    public List<DataModel<Object>> readData(
            final String tableName, final String url,
            final String user, final String password)
            throws Exception {
        return Collections.emptyList();
    }
}
