package com.coda.core.util;

import com.coda.core.entities.DataModel;

import java.util.List;

/**
 * DatabaseExtractor is an interface that contains all the methods that can be used to extract data from a database.
 * @see DatabaseExtractor
 * @see com.coda.core.util.MySQLExtractor
 *
 */

public interface DatabaseExtractor {

    List<DataModel<Object>> readData(String tableName,
     String url, String user, String password) throws Exception;


}
