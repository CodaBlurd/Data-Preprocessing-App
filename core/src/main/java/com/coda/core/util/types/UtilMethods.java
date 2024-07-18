package com.coda.core.util.types;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataLoadingException;
import com.coda.core.util.db.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public final class UtilMethods {

    private UtilMethods() {
        throw new IllegalStateException("Utility class");
    }

    public static void validateTableName(String tableName) {
        if (!isTableNameValid(tableName)) {
            throw new IllegalArgumentException("Invalid table name");
        }
    }

    public static boolean doesTableExist(ConnectionFactory connectionFactory, String tableName) {
        String query = "SELECT COUNT(*) FROM information_schema.tables "
                + "WHERE table_schema = DATABASE() AND table_name = ?";
        try (Connection conn = connectionFactory.dataSource().getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            log.error("Error checking table existence", e);
            return false;
        }
    }

    public static void insertDataModels(ConnectionFactory connectionFactory,
                                        String tableName,
                                        List<DataModel<Object>> dataModels) {
        tableName = sanitizeTableName(tableName);
        Set<String> columns = extractColumns(dataModels, true);
        log.info("Extracted columns: {}", columns);

        if (!doesTableExist(connectionFactory, tableName)) {
            createTable(connectionFactory, tableName, dataModels.get(0).getAttributesMap());
        } else {
            updateTableWithNewColumns(connectionFactory, tableName, dataModels.get(0).getAttributesMap());
        }

        adjustExistingColumns(connectionFactory, tableName);
        Set<String> existingColumns = getExistingColumns(connectionFactory, tableName);

        if (!existingColumns.containsAll(columns)) {
            addMissingColumnsToTable(connectionFactory, dataModels, tableName, columns, existingColumns);
            existingColumns = getExistingColumns(connectionFactory, tableName);
        }

        log.info("Final existing columns: {}", existingColumns);
        String insertSQL = buildInsertSQL(dataModels.get(0), tableName, columns);
        log.info("Executing SQL: {}", insertSQL);

        try (Connection connection = connectionFactory.dataSource().getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            for (DataModel<Object> model : dataModels) {
                log.info("DataModel: {}", model.getAttributesMap());
                setupPreparedStatement(preparedStatement, model.getAttributesMap(), columns);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            log.error("Error while loading data into database", e);
            throw new DataLoadingException("Error while loading data into database: "
                    + e, ErrorType.DATA_LOADING_EXCEPTION);
        }
    }



    private static void adjustExistingColumns(ConnectionFactory connectionFactory, String tableName) {
        Set<String> existingColumns = getExistingColumns(connectionFactory, tableName);
        for (String column : existingColumns) {
            String alterColumnSQL = String.format("ALTER TABLE `%s` MODIFY COLUMN `%s` %s;", tableName,
                    sanitizeColumnName(column), determineSQLTypeForAdjustment(column));
            log.info("Adjusting column: {} to appropriate type using SQL: {}", column, alterColumnSQL);
            try (Connection connection = connectionFactory.dataSource().getConnection();
                 Statement statement = connection.createStatement()) {
                statement.executeUpdate(alterColumnSQL);
                log.info("Adjusted column: {}", column);
            } catch (SQLException e) {
                log.error("Failed to adjust column: {}", column, e);
            }
        }
    }

    private static String determineSQLTypeForAdjustment(String column) {
        if (column.contains("id")) {
            return "INT";
        } else if (column.contains("price")) {
            return "DECIMAL(10,2)";
        } else if (column.contains("created_at")) {
            return "timestamp";
        } else if (column.contains("is_")) {
            return "TINYINT(1)";
        } else {
            return "TEXT";
        }
    }

    private static void addMissingColumnsToTable(ConnectionFactory connectionFactory,
                                                 List<DataModel<Object>> dataModels,
                                                 String tableName, Set<String> columns,
                                                 Set<String> existingColumns) {
        setRowFormatToDynamic(connectionFactory, tableName);
        Set<String> missingColumns = new HashSet<>(columns);
        missingColumns.removeAll(existingColumns);

        for (String column : missingColumns) {
            addMissingColumnSafely(connectionFactory, tableName, column,
                    determineSQLTypeWithCheck(connectionFactory, tableName, dataModels, column));
        }
    }


    private static void addMissingColumnSafely(ConnectionFactory connectionFactory,
                                               String tableName,
                                               String columnName, String columnType) {
        // Check if the column already exists
        if (doesColumnExist(connectionFactory, tableName, columnName)) {
            log.info("Column {} already exists in table {}, skipping addition.", columnName, tableName);
            return;
        }

        // Adjust the column type if necessary
        if (!canAddColumnWithType(connectionFactory, tableName, columnType)) {
            log.warn("Cannot add column {} with type {} due to row size limit. Changing type to TEXT.", columnName, columnType);
            columnType = "TEXT";
        }

        String sanitizedColumnName = sanitizeColumnName(columnName);
        if (doesColumnExist(connectionFactory, tableName, sanitizedColumnName)) {
            log.info("Sanitized column {} already exists in table {}, skipping addition.", sanitizedColumnName, tableName);
            return;
        }

        String alterTableSQL = String.format("ALTER TABLE `%s` ADD COLUMN `%s` %s;", tableName, sanitizedColumnName, columnType);
        log.info("Adding column with SQL: {}", alterTableSQL);
        executeSqlUpdate(connectionFactory, alterTableSQL);
    }


    private static boolean doesColumnExist(ConnectionFactory connectionFactory,
                                           String tableName, String columnName) {
        String query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA "
                + "= DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?";
        try (Connection conn = connectionFactory.dataSource().getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, tableName);
            stmt.setString(2, columnName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            log.error("Error checking column existence", e);
        }
        return false;
    }

    private static boolean canAddColumnWithType(ConnectionFactory connectionFactory, String tableName, String columnType) {
        int maxRowSize = 65535;
        int newColumnLength = columnType.startsWith("VARCHAR") ? Integer.parseInt(columnType.replaceAll("[^0-9]", "")) : 255;

        String query = String.format("SELECT SUM(CHARACTER_MAXIMUM_LENGTH) AS total_length FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'", tableName);
        try (Connection connection = connectionFactory.dataSource().getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {
            if (rs.next()) {
                int totalLength = rs.getInt("total_length");
                return (totalLength + newColumnLength) <= maxRowSize;
            }
        } catch (SQLException e) {
            log.error("Error checking row size", e);
            throw new DataLoadingException("Error while checking row size: " + e, ErrorType.DATA_LOADING_EXCEPTION);
        }
        return false;
    }

    private static void setRowFormatToDynamic(ConnectionFactory connectionFactory,
                                              String tableName) {
        String setRowFormatSQL = String.format("ALTER TABLE `%s` ROW_FORMAT=DYNAMIC;", tableName);
        log.info("Setting ROW_FORMAT to DYNAMIC using SQL: {}", setRowFormatSQL);

        try (Connection connection = connectionFactory.dataSource().getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(setRowFormatSQL);
            log.info("Set ROW_FORMAT to DYNAMIC for table: {}", tableName);
        } catch (SQLException e) {
            log.error("Failed to set ROW_FORMAT to DYNAMIC for table: {}", tableName, e);
            throw new DataLoadingException("Error while setting ROW_FORMAT to DYNAMIC for table: " + tableName, ErrorType.DATA_LOADING_EXCEPTION);
        }
    }

    private static String determineSQLTypeWithCheck(ConnectionFactory connectionFactory, String tableName, List<DataModel<Object>> dataModels, String columnName) {
        String sqlType = determineSQLType(dataModels, columnName);
        if (sqlType.startsWith("VARCHAR") && isRowSizeTooLarge(connectionFactory, tableName, sqlType)) {
            log.warn("Changing column type of {} to TEXT to avoid row size limit.", columnName);
            return "TEXT";
        }
        return sqlType;
    }

    private static boolean isRowSizeTooLarge(ConnectionFactory connectionFactory, String tableName, String newColumnType) {
        String query = String.format("SELECT SUM(CHARACTER_MAXIMUM_LENGTH) AS total_length FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'", tableName);
        try (Connection connection = connectionFactory.dataSource().getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {
            if (rs.next()) {
                int totalLength = rs.getInt("total_length");
                log.info("Total length of columns in table {}: {}", tableName, totalLength);
                int maxRowSize = 65535;
                int newColumnLength = 255;
                if (newColumnType.startsWith("VARCHAR")) {
                    newColumnLength = Integer.parseInt(newColumnType.replaceAll("[^0-9]", ""));
                }
                return (totalLength + newColumnLength) > maxRowSize;
            }
        } catch (SQLException e) {
            log.error("Error checking row size", e);
            throw new DataLoadingException("Error while checking row size: " + e, ErrorType.DATA_LOADING_EXCEPTION);
        }
        return false;
    }

    private static String determineSQLType(List<DataModel<Object>> dataModels, String columnName) {
        for (DataModel<Object> model : dataModels) {
            DataAttributes<Object> attribute = model.getAttributesMap().get(columnName);
            if (attribute != null) {
                return getSQLType(attribute.getValue());
            }
        }
        return "VARCHAR(255)";
    }

    public static void updateTableWithNewColumns(ConnectionFactory connectionFactory,
                                                 String tableName,
                                                 Map<String, DataAttributes<Object>> attributes) {
        if (!doesTableExist(connectionFactory, tableName)) {
            log.error("Table {} does not exist. Attempting to create it.", tableName);
            createTable(connectionFactory, tableName, attributes);
        }

        Set<String> existingColumns = getExistingColumns(connectionFactory, tableName);
        log.info("Existing columns in table {}: {}", tableName, existingColumns);

        for (Map.Entry<String, DataAttributes<Object>> entry : attributes.entrySet()) {
            String columnName = sanitizeColumnName(entry.getKey().replace(' ', '_'));
            if (!existingColumns.contains(columnName)) {
                String sqlType = getSQLType(entry.getValue().getValue());
                String alterTableSQL = String.format("ALTER TABLE `%s` ADD COLUMN `%s` %s;",
                        tableName, columnName, sqlType);
                log.info("Adding new column with SQL: {}", alterTableSQL);
                executeSqlUpdate(connectionFactory, alterTableSQL);
            } else {
                log.info("Column {} already exists in table {}, skipping addition.", columnName, tableName);
            }
        }
    }

    public static void createTable(ConnectionFactory connectionFactory,
                                   String tableName,
                                   Map<String, DataAttributes<Object>> attributes) {
        if (!doesTableExist(connectionFactory, tableName)) {
            Set<String> columnNames = new HashSet<>();
            StringBuilder createTableSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS `")
                    .append(tableName).append("` (");

            boolean first = true;
            for (Map.Entry<String, DataAttributes<Object>> entry
                    : attributes.entrySet()) {
                attributes.forEach((key, value) -> log.info("Key: {}, Value: {}", key, value));
                log.info("Creating column: {}", entry.getKey());
                String columnName
                        = sanitizeColumnName(entry.getKey().replace(' ', '_'));
                if (!columnNames.contains(columnName)) {
                    if (!first) {
                        createTableSQL.append(", ");
                    }
                    createTableSQL.append("`").append(columnName).append("` ")
                            .append(getSQLType(entry.getValue().getValue()));
                    columnNames.add(columnName);
                    first = false;
                } else {
                    log.warn("Duplicate column name avoided: {}", columnName);
                }
            }

            createTableSQL.append(");");
            log.info("Creating table with SQL: {}", createTableSQL.toString());
            executeSqlUpdate(connectionFactory, createTableSQL.toString());
        } else {
            log.info("Table {} already exists, skipping creation.", tableName);
        }
    }

    public static List<DataModel<Object>> extractDataModels(PreparedStatement preparedStatement)
            throws SQLException {
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            List<DataModel<Object>> dataModels = new ArrayList<>();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                Map<String, DataAttributes<Object>> attributes = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = resultSet.getObject(i);
                    attributes.put(columnName, new DataAttributes<>(columnName, value,
                            value.getClass().getName(), Object.class));
                }

                String idStr = resultSet.getString("id");
                ObjectId objectId = (idStr != null && ObjectId.isValid(idStr))
                        ? new ObjectId(idStr) : new ObjectId();
                dataModels.add(new DataModel<>(objectId, attributes));
            }
            return dataModels;
        } catch (SQLException e) {
            log.error("Error processing result set", e);
            throw new DataLoadingException("Error processing result set: "
                    + e, ErrorType.DATA_LOADING_EXCEPTION);
        }
    }

    private static void setupPreparedStatement(PreparedStatement stmt,
                                               Map<String, DataAttributes<Object>> attributes,
                                               Set<String> columns) throws SQLException {
        int index = 1;

        Map<String, Object> combinedAttributes = new HashMap<>();
        attributes.forEach((column, attr) -> {
            if (attr != null && attr.getValue() != null) {
                combinedAttributes.put(column, attr.getValue());
            }
            assert attr != null;
            if (attr.getEncodedValues() != null) {
                combinedAttributes.putAll(attr.getEncodedValues());
            }
        });

        for (String column : columns) {
            Object value = combinedAttributes.get(column);
            if (value != null) {
                stmt.setObject(index++, value);
            } else {
                stmt.setNull(index++, Types.NULL);
            }
        }
    }

    private static Set<String> extractColumns(List<DataModel<Object>> dataModels,
                                              boolean includeEncoded) {
        Set<String> columns = new HashSet<>();
        dataModels.forEach(dataModel -> {
            dataModel.getAttributesMap().forEach((key, value) -> {
                columns.add(sanitizeKey(key));
                if (includeEncoded && value.getEncodedValues() != null) {
                    value.getEncodedValues().keySet().forEach(encodedKey
                            -> columns.add(sanitizeKey(encodedKey)));
                }
            });
        });
        return columns;
    }

    private static Set<String> getExistingColumns(ConnectionFactory connectionFactory, String tableName) {
        String query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?";
        try (Connection connection = connectionFactory.dataSource().getConnection();
             PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                Set<String> columns = new HashSet<>();
                while (rs.next()) {
                    columns.add(rs.getString("COLUMN_NAME"));
                }
                return columns;
            }
        } catch (SQLException e) {
            log.error("Error retrieving existing columns", e);
            return Collections.emptySet();
        }
    }

    private static void executeSqlUpdate(ConnectionFactory connectionFactory, String sql) {
        try (Connection connection = connectionFactory.dataSource().getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            log.error("SQL update failed: {}. Error: {}", sql, e.getMessage(), e);
            throw new DataLoadingException("SQL update failed: "
                    + sql, ErrorType.DATA_LOADING_EXCEPTION);
        }
    }

    private static String sanitizeTableName(String tableName) {
        return tableName.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    public static String sanitizeKey(String key) {
        return key.replace('.', '_').replace(' ', '_').replace('-', '_');
    }

    private static String sanitizeColumnName(String columnName) {
        return columnName.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    private static String getSQLType(Object value) {
        if (value instanceof Integer) return "INT";
        if (value instanceof String) {
            if (((String) value).length() > 255) return "TEXT";
            return "VARCHAR(255)";
        }
        if (value instanceof Double) return "DECIMAL(10,2)";
        if (value instanceof Boolean) return "TINYINT(1)";
        if (value instanceof Long) return "BIGINT";
        if (value instanceof BigDecimal) return "DECIMAL(10,2)";
        if (value instanceof Timestamp) return "timestamp";
        return "TEXT";
    }

    private static String buildInsertSQL(DataModel<Object> dataModel, String tableName, Set<String> columns) {
        String columnList = columns.stream()
                .map(UtilMethods::sanitizeColumnName)
                .collect(Collectors.joining("`, `", "`", "`"));
        String placeholders = String.join(", ", Collections.nCopies(columns.size(), "?"));

        StringBuilder query = new StringBuilder("INSERT INTO ");
        query.append(tableName)
                .append(" (")
                .append(columnList)
                .append(") VALUES (")
                .append(placeholders)
                .append(") ON DUPLICATE KEY UPDATE ");

        String updateClause = columns.stream()
                .map(column -> "`" + sanitizeColumnName(column)
                        + "`=VALUES(`"
                        + sanitizeColumnName(column) + "`)")
                .collect(Collectors.joining(", "));

        query.append(updateClause);
        return query.toString();
    }

    private static boolean isTableNameValid(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            return false;
        }
        String regex = "^[a-zA-Z0-9_]+$";
        return tableName.matches(regex);
    }
}
