package com.coda.core.service;

import com.coda.core.batch.processor.DataModelProcessor;
import com.coda.core.config.MySQLProperties;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataExtractionException;
import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.db.*;
import com.coda.core.util.transform.DataTransformation;
import com.coda.core.util.types.ErrorType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.coda.core.util.Constants.BATCH_SIZE;

@Slf4j
@Component
public class ServiceMethodsTest {

    private final MySQLExtractor mySQLExtractor;
    private final ConnectionFactory connectionFactory;
    private static MongoDBExtractor mongoDBExtractor;
    private final DataModelProcessor dataModelProcessor;
    private final DataTransformation dataTransformation;

    @Autowired
    private static MySQLProperties mySQLProperties;
    private final DatabaseExtractorFactory databaseExtractorFactory;

    private static Iterator<DataModel<Object>> dataModelIterator;

    private final DataModelRepository dataModelRepository;

    public ServiceMethodsTest(MySQLExtractor mySQLExtractor,
                              ConnectionFactory connectionFactory,
                              MongoDBExtractor mongoDBExtractor,
                              DataModelProcessor processor,
                              DataTransformation dataTransformation,
                              DataModelRepository dataModelRepository) {
        this.mySQLExtractor = mySQLExtractor;
        this.connectionFactory = connectionFactory;
        ServiceMethodsTest.mongoDBExtractor = mongoDBExtractor;
        this.databaseExtractorFactory
                = new DatabaseExtractorFactory(connectionFactory,
                mySQLExtractor, mongoDBExtractor);
        this.dataTransformation = dataTransformation;
        this.dataModelProcessor = processor;
        this.dataModelRepository = dataModelRepository;
    }

    public static void main(String[] args) {
        System.out.println("Hello, World!");

        // Initialize dependencies
        MySQLProperties mySQLProperties = new MySQLProperties();
        mySQLProperties.setUrl("jdbc:mysql://localhost:3306/mydatabase?useSSL=false&serverTimezone=UTC");
        mySQLProperties.setUsername("myuser");
        mySQLProperties.setPassword("mypassword");
        mySQLProperties.setDriverClassName("com.mysql.cj.jdbc.Driver");
        mySQLProperties.setMaximumPoolSize(5);

        DataModelRepository dmRepository = new RepoTestImpl();
        ConnectionFactory connectionFactory = new SqlDbConnectionFactory(mySQLProperties);
        MySQLExtractor mySQLExtractor = new MySQLExtractor();
        mySQLExtractor.setConnectionFactory(connectionFactory);

        DataTransformation dataTransformation = new DataTransformation();
        DataModelProcessor dataModelProcessor = new DataModelProcessor(dataTransformation);


        // Create an instance of ServiceMethodsTest
        ServiceMethodsTest serviceMethodsTest = new ServiceMethodsTest(mySQLExtractor,
                connectionFactory, mongoDBExtractor,
                dataModelProcessor, dataTransformation, dmRepository);

        List<DataModel<Object>> dataModels = null;
        try {
            dataModels = serviceMethodsTest.extractDataFromTable("mysql",
                    "products");
            dataModelProcessor.processAndSaveDataModels(dataModels,
                    BATCH_SIZE,
                    dmRepository);

            dataModelIterator = dataModels.iterator();
            while (dataModelIterator.hasNext()) {
                DataModel<Object> dataModel = dataModelIterator.next();
                for (Object value : dataModel.getAttributesMap().values()) {
                    System.out.println(value);
                }
            }
        } catch (ReadFromDbExceptions e) {
            log.error("Error while reading data from database: {}, Cause: {}",
                    e.getMessage(), "{}", e.getCause());
        } catch (DataExtractionException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        // Load data to MySQL
        serviceMethodsTest.loadDataToMySQL(dataModels, "products", "mysql");
    }

    public List<DataModel<Object>> extractDataFromTable(final String type, final String tableName) throws ReadFromDbExceptions {
        validateArguments(type, tableName);

        try {
            DatabaseExtractor databaseExtractor = databaseExtractorFactory.getExtractor(type.trim().toLowerCase());
            Objects.requireNonNull(databaseExtractor, "No suitable extractor for provided db type found");
            List<DataModel<Object>> allDataModels = new ArrayList<>();
            int offSet = 0;
            while (true) {
                List<DataModel<Object>> dataModels = databaseExtractor.readData(tableName, BATCH_SIZE, offSet);

                if (dataModels.isEmpty()) {
                    break;
                }

                log.info("Extracted {} data models from table: {}", dataModels.size(), tableName);

                allDataModels.addAll(dataModels);
                offSet += BATCH_SIZE;
            }

            return allDataModels;

        } catch (IOException e) {
            log.error("Error while reading data from database: {}, Cause: {}", e.getMessage(), "{}", e.getCause());
            throw new ReadFromDbExceptions("Error reading from database: " + e.getMessage(), ErrorType.READ_FROM_DB_EXCEPTIONS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void loadDataToMySQL(List<DataModel<Object>> dataModels,
                                String tableName, String type) {
        validateArguments(dataModels, tableName, type);
        try{
            DatabaseExtractor databaseExtractor = databaseExtractorFactory.getExtractor(type.trim().toLowerCase());
            Objects.requireNonNull(databaseExtractor, "No suitable extractor for provided db type found");
            databaseExtractor.loadData(dataModels, tableName);
            log.info("Data loaded to MySQL successfully");
        } catch (IOException e) {
            log.error("Error while loading data to database: {}, Cause: {}", e.getMessage(), "{}", e.getCause());
            throw new ReadFromDbExceptions("Error loading data to database: " + e.getMessage(), ErrorType.READ_FROM_DB_EXCEPTIONS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private static void validateArguments(final Object... args) {
        for (Object arg : args) {
            if (arg == null || (arg instanceof String && ((String) arg).isEmpty())) {
                throw new IllegalArgumentException("Invalid argument: " + arg);
            }
        }
    }
}
