package com.coda.core.util.db;

import com.coda.core.dtos.ConnectionDetails;
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
     * configureDataSource().
     * @param connectionDetails The connection details provided by the user.
     */
     void configureDataSource(ConnectionDetails connectionDetails);


    /**
     <p>
     This method is used to extract data
     from a relational database.
     </p>
     * @param tableName The name of the table to be extracted.
     * @param batchSize The number of rows to be extracted.
     * @param offSet The number of rows to be skipped.
     * @return A list of DataModel objects.
     * @throws Exception if the table name is invalid.
     */

    List<DataModel<Object>> readData(String tableName,
                                     int batchSize,
                                     int offSet) throws Exception;

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

    /**
     * loadData().
     * This method loads data to  the database.
     * @param dataModels A list of DataModel objects.
     * @param tableName The name of the table to be loaded.
     * @throws Exception if the data is invalid.
     */

    void loadData(List<DataModel<Object>> dataModels, String tableName)
            throws Exception;

    /**
     * loadData().
     * This method loads data to  the database.
     * @param dataModels A map of DataModel documents.
     * @param dbName The name of the database.
     * @param tableName The name of the table to be loaded.
     * @param url The url of the database.
     * @throws Exception if the data is invalid.
     */

    void loadData(Map<String, DataModel<Document>> dataModels,
                  String dbName, String tableName, String url) throws Exception;



}
