package com.coda.core.util.db;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.util.types.ErrorType;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * This class is used to extract data from a mysql type database.
 * <p> This class is used to extract data from a mysql type database</p>
 */
@Slf4j
public final class MySQLExtractor implements DatabaseExtractor {

    /**
     * The connection factory to be used to connect to the database.
     * <p> The connection factory to be used to connect to the database.</p>
     */
    private final ConnectionFactory connectionFactory
            = new SqlDbConnectionFactory();

    /**
     * This method is used to extract data from a mysql type database.
     * @param tableName The name of the table to be extracted.
     * @param url The url of the database.
     * @param user The username of the database.
     * @param password The password of the database.
     * @return A list of DataModel objects.
     * @throws Exception if the table name is invalid.
     */

    @Override
    public List<DataModel<Object>> readData(
            final String tableName, final String url,
            final String user, final String password) throws Exception {
        // Validate or sanitize tableName to ensure it's safe to use in a query
        if (!isTableNameValid(tableName)) {
            throw new IllegalArgumentException("Invalid table name");
        }

        String query = String.format(Queries.READ_FROM_MYSQL, tableName);

        try (Connection connection
                     = connectionFactory.connect(url, user, password);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            List<DataModel<Object>> dataModels = new ArrayList<>();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            // get column names and data types
            List<String> columnNames = new ArrayList<>();
            List<String> dataTypes = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(metaData.getColumnName(i));
                dataTypes.add(metaData.getColumnTypeName(i));
            }

            while (resultSet.next()) {
                Map<String, DataAttributes<Object>> attributes
                        = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    DataAttributes<Object> attribute = new DataAttributes<>(
                            metaData.getColumnName(i),
                            resultSet.getObject(i),
                            metaData.getColumnTypeName(i),
                            Object.class
                    );
                    attributes.put(metaData.getColumnName(i), attribute);
                }
                String idStr = resultSet.getString("id");
                ObjectId objectId
                        = ObjectId.isValid(idStr)
                        ? new ObjectId(idStr) : new ObjectId();

                DataModel<Object> dataModel
                        = new DataModel<>(objectId, attributes);
                dataModels.add(dataModel);
            }



            return dataModels;
        } catch (SQLException e) {
            log.error("Error while reading data from database", e);
            throw new ReadFromDbExceptions(
                    "Error while reading data from database",
                    ErrorType.READ_FROM_DB_EXCEPTIONS);
        }
    }

    /**
     <p>This method is used to validate the table name.
     to protect against SQL injection. </p>
     * @param tableName The name of the table to be validated.
     * @return A boolean value.
     */

private boolean isTableNameValid(final String tableName) {
    // Check if tableName is not null and not empty
    if (tableName == null
            || tableName.trim().isEmpty()) {
        return false;
    }

    // Check if tableName contains only alphanumeric characters and underscores
    String regex = "^[a-zA-Z0-9_]+$"; //
    if (!tableName.matches(regex)) {
        return false;
    }

    return true;
}

// == Not used for this class, but required to implement the interface ==

    @Override
    public Map<String, DataModel<Document>> readData(final String databaseName,
   final String tableName, final String url) {
        return new HashMap<>();
    }
}
