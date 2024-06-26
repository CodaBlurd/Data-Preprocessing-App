package com.coda.core.util.db;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataLoadingException;
import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.util.types.ErrorType;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.coda.core.util.types.MySQLDataTypes.BIGINT;
import static com.coda.core.util.types.MySQLDataTypes.DECIMAL;
import static com.coda.core.util.types.MySQLDataTypes.INTEGER;
import static com.coda.core.util.types.MySQLDataTypes.VARCHAR;
import static com.coda.core.util.types.MySQLDataTypes.BOOLEAN;
import static com.coda.core.util.types.MySQLDataTypes.DOUBLE;
import static com.coda.core.util.types.MySQLDataTypes.FLOAT;
import static com.coda.core.util.types.MySQLDataTypes.TEXT;
import static com.coda.core.util.types.MySQLDataTypes.CHAR;
import static com.coda.core.util.types.MySQLDataTypes.INT;


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
     * @param factory the connection
     * factory object.
     */
    public void setConnectionFactory(final ConnectionFactory factory) {
        this.connectionFactory = factory;
    }

    /**
     * This method is used to extract data from a mysql type database.
     * @param tableName The name of the table to be extracted.
     * @return A list of DataModel objects.
     */

    @Override
    public List<DataModel<Object>> readData(
            final  String tableName,
            final int batchSize, final int offSet)  {
        // Validate or sanitize tableName to ensure it's safe to use in a query
        if (!isTableNameValid(tableName)) {
            throw new IllegalArgumentException("Invalid table name");
        }

        String query = String.format(Queries.READ_FROM_MYSQL, tableName);

        try (Connection connection
                     = connectionFactory.dataSource().getConnection();
             PreparedStatement preparedStatement
                     = connection.prepareStatement(query)) {

            // Set the parameters for LIMIT and OFFSET
            preparedStatement.setInt(1, batchSize);
            preparedStatement.setInt(2, offSet);

            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                List<DataModel<Object>> dataModels = new ArrayList<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                // Get column names and data types
                List<String> columnNames
                        = new ArrayList<>(columnCount);

                List<String> dataTypes
                        = new ArrayList<>(columnCount);

                for (int i = 1; i <= columnCount; i++) {
                    columnNames.add(metaData.getColumnName(i));
                    dataTypes.add(metaData
                            .getColumnTypeName(i).equals("DECIMAL")
                            ? "BigDecimal" : metaData.getColumnTypeName(i));
                }

                while (resultSet.next()) {
                    Map<String, DataAttributes<Object>> attributes
                            = new HashMap<>(columnCount);

                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = resultSet.getObject(i);
                        String columnType = metaData.getColumnTypeName(i);
                        String javaType = mapSqlTypeToJavaType(columnType);
                        Class<?> clazz = Class.forName(javaType);

                        if (columnType.equals("DECIMAL")) {
                            value = resultSet.getBigDecimal(i);
                        }

                        DataAttributes<?> attribute = new DataAttributes<>(
                                columnName,
                                value,
                                javaType,
                                clazz
                        );
                        attributes.put(metaData.getColumnName(i),
                                (DataAttributes<Object>) attribute);
                    }

                    String idStr = resultSet.getString("id");
                    ObjectId objectId = (idStr != null
                            && ObjectId.isValid(idStr))
                            ? new ObjectId(idStr) : new ObjectId();

                    DataModel<Object> dataModel
                            = new DataModel<>(objectId, attributes);
                    dataModels.add(dataModel);
                }

                log.info("Data read from table: {}", tableName);
                return dataModels;
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            log.error("Error while reading data from database", e);
            throw new ReadFromDbExceptions("Error while "
                    + "reading data from database: " + e.getMessage(),
                    ErrorType.READ_FROM_DB_EXCEPTIONS);
        }
    }

    /**
     * This method is used to map sql types to java types.
     * @param sqlType The sql type to be mapped.
     * @return The java type corresponding to the sql type.
     */

    private String mapSqlTypeToJavaType(final String sqlType) {
        return switch (sqlType) {
            case  INT, INTEGER -> Integer.class.getName();
            case DOUBLE -> Double.class.getName();
            case FLOAT -> Float.class.getName();
            case BIGINT -> Long.class.getName();
            case DECIMAL -> BigDecimal.class.getName();
            case VARCHAR, CHAR, TEXT -> String.class.getName();
            case BOOLEAN -> Boolean.class.getName();
            default -> Object.class.getName();
        };
    }

    //== Data Loading .. ==

    /**
     * This method is used to load data into a mysql type database.
     * @param dataModels The list of DataModel objects to be loaded.
     * @param tableName The name of the table to be loaded.
     */

    @Override
    public void loadData(final List<DataModel<Object>> dataModels,
                         final String tableName) {
        if (dataModels == null || dataModels.isEmpty()) {
            throw new IllegalArgumentException("dataModels cannot "
                    + "be null or empty");
        }

        String insertSQL = buildInsertSQL(dataModels.get(0), tableName);

        try (Connection connection = connectionFactory.dataSource()
                .getConnection();
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
            log.info("Inserted {} records into {}",
                    updateCounts.length, tableName);

        } catch (SQLException e) {
            log.error("Error while loading data into"
                    + " database {}", e.getMessage());
            throw new DataLoadingException("Error while loading data"
                    + " into database: "
                    + e, ErrorType.DATA_LOADING_EXCEPTION);
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

        query.delete(query.length() - 2, query.length());
        query.append(") VALUES (");

        query.append("?, ".repeat(attributes.size()));

        query.delete(query.length() - 2, query.length());
        query.append(") ON DUPLICATE KEY UPDATE ");

        for (String attributeName : attributes.keySet()) {
            query.append(attributeName)
                    .append("=VALUES(").append(attributeName)
                    .append("), ");
        }

        query.delete(query.length() - 2, query.length());

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
    public void loadData(final Map<String,
            DataModel<Document>> dataModels,
                         final String databaseName,
                         final String tableName,
                         final String url) {

    }
}
