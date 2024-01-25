package com.coda.core.util.db;

import com.coda.core.entities.DataModel;
import org.bson.Document;

import java.util.List;
import java.util.Map;

/**
 * DatabaseExtractor is an interface that contains all the methods that can be used to extract data from a database.
 * @see DatabaseExtractor
 * @see MySQLExtractor
 *
 */

public interface DatabaseExtractor {

    List<DataModel<Object>> readData(String tableName,
     String url, String user, String password) throws Exception;

    Map<String, DataModel<Document>> readData(String databaseName,
      String tableName, String url);



}
