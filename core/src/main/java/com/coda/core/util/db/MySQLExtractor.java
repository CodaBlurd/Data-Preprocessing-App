package com.coda.core.util.db;

import com.coda.core.batch.processor.DataModelProcessor;
import com.coda.core.dtos.ConnectionDetails;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.service.DataRepoImpl;
import com.coda.core.util.transform.DataTransformation;
import com.coda.core.util.types.ErrorType;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.coda.core.util.types.UtilMethods.*;

@Slf4j
public final class MySQLExtractor implements DatabaseExtractor {

    private ConnectionFactory connectionFactory;

    public static void main(String[] args) {
        DataRepoImpl dataRepo = new DataRepoImpl();
        ConnectionFactory connectionFactory = new SqlDbConnectionFactory();
        DataTransformation transformation = new DataTransformation();
        DataModelProcessor processor = new DataModelProcessor(transformation);
        MySQLExtractor extractor = new MySQLExtractor();
        ConnectionDetails connectionDetails = new ConnectionDetails(
                "jdbc:mysql://localhost:3306/mydatabase?useSSL=false&serverTimezone=UTC",
                "myuser",
                "mypassword"
        );
        connectionFactory.createDataSource(connectionDetails);
        extractor.setConnectionFactory(connectionFactory);
        List<DataModel<Object>> dataModels
                = extractor.readData("products_new",
                100, 0);
        dataModels.forEach(dataModel -> log.info(dataModel.toString()));
        try {
            processor.processAndSaveDataModels(dataModels,
                    100, dataRepo );
        } catch (Exception e) {
            log.error("Error while processing data", e);
        }


        extractor.loadData(dataModels, "products_copy13");

        extractor.drawTableAndDisplayLoadedData();






    }
    public void setConnectionFactory(final ConnectionFactory factory) {
        this.connectionFactory = factory;
    }

    @Override
    public void configureDataSource(ConnectionDetails connectionDetails) {
        connectionFactory.createDataSource(connectionDetails);
    }

    /**
     * Read data from the database.
     * @param tableName The name of the table to be extracted.
     * @param batchSize The number of rows to be extracted.
     * @param offSet The number of rows to be skipped.
     * @return The list of data models.
     */

    @Override
    public List<DataModel<Object>> readData(final String tableName,
                                            final int batchSize, final int offSet) {
        validateTableName(tableName);

        String query = String.format(Queries.READ_FROM_MYSQL, tableName);
        try (Connection connection = connectionFactory.dataSource().getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            preparedStatement.setInt(1, batchSize);
            preparedStatement.setInt(2, offSet);

            return extractDataModels(preparedStatement);
        } catch (SQLException e) {
            log.error("Error while reading data from database", e);
            throw new ReadFromDbExceptions("Error while reading data from database: "
                    + e.getMessage(),
                    ErrorType.READ_FROM_DB_EXCEPTIONS);
        }
    }


    /**
     * Load data into the database.
     * @param dataModels A list of DataModel objects.
     * @param targetTableName The name of the table to be loaded.
     */

    @Override
    public void loadData(final List<DataModel<Object>> dataModels,
                         final String targetTableName) {
        if (dataModels == null || dataModels.isEmpty()) {
            throw new IllegalArgumentException("DataModels cannot be null or empty");
        }

        dataModels.forEach(dataModel -> {
            log.info("DataModel: {}", dataModel.toString());
        });

        log.info("Loading data into table: {}", targetTableName);

        if (doesTableExist(connectionFactory, targetTableName)) {
            log.info("Table {} does not exist. Creating table.", targetTableName);
            createTable(connectionFactory, targetTableName, dataModels.get(0).getAttributesMap());
        }

        log.info("Updating table {} with new columns if any.", targetTableName);
        updateTableWithNewColumns(connectionFactory, targetTableName,
                dataModels.get(0).getAttributesMap());

        log.info("Inserting data models into table: {}", targetTableName);
        insertDataModels(connectionFactory, targetTableName, dataModels);
    }

//    @Override
//    public void loadData(final List<DataModel<Object>> dataModels, final String targetTableName) {
//        insertDataModels(connectionFactory, targetTableName, dataModels);
//    }

public void drawTableAndDisplayLoadedData() {
        try (Connection connection = connectionFactory.dataSource().getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(Queries.SELECT_ALL_FROM_PRODUCTS_COPY10)) {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(metaData.getColumnName(i) + " | ");
                    }
                    System.out.println();
                    while (resultSet.next()) {
                        for (int i = 1; i <= columnCount; i++) {
                            System.out.print(resultSet.getString(i) + " | ");
                        }
                        System.out.println();
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Error while drawing table and displaying loaded data", e);
        }
    }




    // Not implemented for MySQLExtractor

    /**
     * Read data from the document database.
     * @param databaseName The name of the database.
     * @param tableName The name of the table to be extracted.
     * @param url The URL of the database.
     * @return A map of DataModel Documents.
     */

    @Override
    public Map<String, DataModel<Document>> readData(String databaseName,
                                                     String tableName, String url) {
        return Collections.emptyMap(); }

    /**
     * Load data into the document database.
     * @param dataModels A map of DataModel Documents.
     * @param dbName The name of the database.
     * @param tableName The name of the table to be loaded.
     * @param url The URL of the database.
     */

    @Override
    public void loadData(Map<String, DataModel<Document>> dataModels,
                         String dbName, String tableName,
                         String url) { }









}
