package com.coda.core.util.db;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;

/**
 * MongoDBExtractor is a class that contains all the methods that can be used to extract data from a MongoDB database.
 * @see MongoDBExtractor
 */
public class MongoDBExtractor implements DatabaseExtractor {

    /**
     * This method returns a list of DataModel objects that contain the data from the table provided.
     * @param tableName The name of the table to be extracted
     * @param url The url of the database
     * @return List<DataModel<Object>>
     */
    @Override
    public Map<String, DataModel<Document>> readData(String databaseName, String tableName, String url) {
        if (databaseName == null || tableName == null || url == null) {
            throw new IllegalArgumentException("Invalid arguments");
        } else if (databaseName.isEmpty() || tableName.isEmpty() || url.isEmpty()) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        MongoDBConnectionFactory mongoDBConnectionFactory = new MongoDBConnectionFactory(url);
        MongoDatabase mongoDatabase = mongoDBConnectionFactory.getConnection(databaseName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

        Map<String, DataModel<Document>> dataModelList = new HashMap<>();
        for (Document document : collection.find()) {
            DataModel<Document> dataModel = new DataModel<>();
            Map<String, DataAttributes<Document>> attributes = new HashMap<>();
            for (String key : document.keySet()) {
                DataAttributes<Document> attribute = new DataAttributes<>(key, document.get(key), "Document",
                        Document.class);
                attributes.put(key, attribute);
            }
            dataModel.setAttributesMap(attributes);
            dataModelList.put(document.get("_id").toString(), dataModel);
        }

        mongoDBConnectionFactory.close();
        return dataModelList;
    }

    //== not used for this class but required in order to implement DatabaseExtractor interface ==
    @Override
    public List<DataModel<Object>> readData(String tableName, String url, String user, String password) throws Exception {
        return Collections.emptyList();
    }
}
