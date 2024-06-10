package com.coda.core.util.db;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.util.types.ErrorType;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
    private ConnectionFactory connectionFactory;

    /**
     * setConnectionFactory().
     * @param connectionObject the connection
     * factory object.
     */
    public void setConnectionFactory(final ConnectionFactory connectionObject) {
        this.connectionFactory = connectionObject;
    }

    /**
     * This method is used to extract data from a mysql type database.
     * @param tableName The name of the table to be extracted.
     * @return A list of DataModel objects.
     * @throws Exception if the table name is invalid.
     */

    @Override
    public List<DataModel<Object>> readData(final String tableName)
            throws Exception {
        // Validate or sanitize tableName to ensure it's safe to use in a query
        if (!isTableNameValid(tableName)) {
            throw new IllegalArgumentException("Invalid table name");
        }

        String query = String.format(Queries.READ_FROM_MYSQL, tableName);

        try (Connection connection = connectionFactory.connectToMySQL();
             PreparedStatement preparedStatement
                     = connection.prepareStatement(query);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            List<DataModel<Object>> dataModels = new ArrayList<>();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Get column names and data types
            List<String> columnNames = new ArrayList<>(columnCount);
            List<String> dataTypes = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(metaData.getColumnName(i));
                dataTypes.add(metaData.getColumnTypeName(i));
            }

            while (resultSet.next()) {
                Map<String, DataAttributes<Object>> attributes
                        = new HashMap<>(columnCount);
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
                ObjectId objectId = (idStr != null
                        && ObjectId.isValid(idStr))
                        ? new ObjectId(idStr) : new ObjectId();

                DataModel<Object> dataModel
                        = new DataModel<>(objectId, attributes);
                dataModels.add(dataModel);
            }

            return dataModels;
        } catch (SQLException e) {
            log.error("Error while reading data from database", e);
            throw new ReadFromDbExceptions("Error while reading "
                    + "data from database: " + e.getMessage(),
                    ErrorType.READ_FROM_DB_EXCEPTIONS);
        }
    }

    //== Data Loading .. ==

    /**
     * This method is used to load data into a mysql type database.
     * @param dataModels The list of DataModel objects to be loaded.
     * @param tableName The name of the table to be loaded.
     * @throws Exception if the table name is invalid.
     */

    @Override
    public void loadData(final List<DataModel<Object>> dataModels,
                         final String tableName) throws Exception {
        if (dataModels == null || dataModels.isEmpty()) {
            throw new IllegalArgumentException("dataModels"
                    + " cannot be null or empty");
        }

        String insertSQL = buildInsertSQL(dataModels.get(0), tableName);

        try (Connection connection
                     = connectionFactory.connectToMySQL();
             PreparedStatement preparedStatement
                     = connection.prepareStatement(insertSQL)) {

            for (DataModel<Object> dataModel : dataModels) {
                Map<String, DataAttributes<Object>> attributes
                        = dataModel.getAttributesMap();
                int index = 1;
                for (DataAttributes<Object> attribute : attributes.values()) {
                    preparedStatement.setObject(index++, attribute.getValue());
                }
                preparedStatement.addBatch();
            }

            int[] updateCounts = preparedStatement.executeBatch();
            if (updateCounts == null) {
                log.error("executeBatch returned null for table {}", tableName);
            } else {
                log.info("Inserted {} records into {}",
                        updateCounts.length, tableName);
            }

        } catch (SQLException e) {
            log.error("Error while loading data into database", e);
            throw e;
        }
    }


    private String buildInsertSQL(final DataModel<Object> dataModel,
                                  final String tableName) {
        StringBuilder query = new StringBuilder("INSERT INTO ");
        query.append(tableName).append(" (");

        Map<String, DataAttributes<Object>> attributes
                = dataModel.getAttributesMap();
        for (String attributeName : attributes.keySet()) {
            query.append(attributeName).append(", ");
        }

        // Remove the last comma and space
        query.delete(query.length() - 2, query.length());
        query.append(") VALUES (");

        // Add placeholders for the values
        for (int i = 0; i < attributes.size(); i++) {
            query.append("?, ");
        }

        // Remove the last comma and space
        query.delete(query.length() - 2, query.length());
        query.append(")");

        return query.toString();
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
    return tableName.matches(regex);
}



// == Not used for this class, but required to implement the interface ==

    @Override
    public Map<String, DataModel<Document>> readData(
            final String databaseName,
            final String tableName,
            final String url) {
        return new HashMap<>();
    }

    // == Not used for this class, but required to implement the interface ==

    @Override
    public void loadData(final Map<String, DataModel<Document>> dataModels,
                         final String databaseName,
                         final String tableName,
                         final String url)
            throws Exception {

    }
}
