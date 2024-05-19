package com.coda.core.util.db;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;

/**
 <p> MongoDBExtractor is a class that contains
 all the methods that can be used to
 extract data from a MongoDB database.
 </p>
 * @see MongoDBExtractor
 */
public final class MongoDBExtractor implements DatabaseExtractor {

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

        MongoDBConnectionFactory mongoDBConnectionFactory
                = new MongoDBConnectionFactory(url);
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
