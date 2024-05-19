package com.coda.core.util.db;

import com.coda.core.entities.DataModel;
import org.bson.Document;

import java.util.List;
import java.util.Map;

/**
 <p>DatabaseExtractor is
  an interface that contains all
 the methods that can be used
 to extract data from a database.
 </p>
 * @see DatabaseExtractor
 * @see MySQLExtractor
 *
 */

public interface DatabaseExtractor {

    /**
     <p>
     This method is used to extract data
     from a relational database.
     </p>
     * @param tableName The name of the table to be extracted.
     * @param url The url of the database.
     * @param user The username of the database.
     * @param password The password of the database.
     * @return A list of DataModel objects.
     * @throws Exception if the table name is invalid.
     */

    List<DataModel<Object>> readData(String tableName,
     String url, String user, String password) throws Exception;

    /**
    <p>
     This method is used to extract data
     from a MongoDB database.
    </p>
     * @param databaseName The name of the database.
     * @param tableName The name of the table to be extracted.
     * @param url The url of the database.
     * @return A map of DataModel documents.
     */
    Map<String, DataModel<Document>> readData(String databaseName,
      String tableName, String url);



}
