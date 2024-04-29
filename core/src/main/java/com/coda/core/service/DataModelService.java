package com.coda.core.service;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataExtractionException;
import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.*;
import com.coda.core.util.db.DatabaseExtractor;
import com.coda.core.util.db.DatabaseExtractorFactory;
import com.coda.core.util.file.FileExtractor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Service class for DataModel entity
 * <p> This class is responsible for all the business logic related to DataModel entity</p>
 * {@code @Service} annotation to mark the class as a service class
 * {@code @Slf4j} annotation to enable logging
 * {@code @Autowired} annotation to inject the DataModelRepository
 */

@Service
@Slf4j
public class DataModelService {
    private final DataModelRepository dataModelRepository;
    private final DatabaseExtractorFactory databaseExtractorFactory;
    private final FileExtractor fileExtractor;
    @Autowired
    public DataModelService(DataModelRepository dataModelRepository,
                            DatabaseExtractorFactory databaseExtractorFactory,
                            FileExtractor fileExtractor) {
        this.dataModelRepository = dataModelRepository;
        this.databaseExtractorFactory = databaseExtractorFactory;
        this.fileExtractor = fileExtractor;
    }

    //== public methods ==

    //== DATA EXTRACTION PHASE ==

    /**
     * Reads data from a relational database.
     * @param tableName The name of the table to read from
     * @param type The type of the database
     *  The table name must be a valid table name
     * @param url The url of the database
     * @param user The username of the database
     * @param password The password of the database
     * @return A list of DataModel objects
     * @throws ReadFromDbExceptions if the table name is invalid
     */

    @Transactional
    public List<DataModel<Object>> extractDataFromTable(String type, String tableName, String url, String user, String password)
            throws ReadFromDbExceptions {
        if (type == null || tableName == null || url == null || user == null || password == null) {
            throw new IllegalArgumentException("Invalid arguments");
        }
        // extract data from the database
        try{
            // check the database type
            DatabaseExtractor databaseExtractor =   databaseExtractorFactory.getExtractor(type);
            List<DataModel<Object>> dataModels = databaseExtractor.readData(tableName, url, user, password);
            extract(dataModels);
            dataModelRepository.saveAll(dataModels);
            return dataModels;
        } catch (Exception e) {
            log.error("Error while reading data from database", e);
            throw new ReadFromDbExceptions("Error reading from database",
                    ErrorType.READ_FROM_DB_EXCEPTIONS);
        }
    }

    /**
     * Reads data from a non-relational database.
     * @param tableName The name of the table to read from
     * @param url The url of the database
     * @param type The type of the database
     * @param databaseName The name of the database
     * @return A map of DataModel objects with the key being the id or key of the document
     * @throws ReadFromDbExceptions if the table name is invalid
     */

    public Map<String, DataModel<Document>> extractDataFromTable(String type, String databaseName, String tableName, String url)
            throws ReadFromDbExceptions {
        if (type == null || tableName == null || url == null) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        try {
            DatabaseExtractor databaseExtractor = databaseExtractorFactory.getExtractor(type);
            Map<String, DataModel<Document>> dataModels = databaseExtractor.readData(databaseName, tableName, url);
            if (dataModels != null) {
                for (DataModel<Document> dataModel : dataModels.values()) {
                    if (dataModel.getAttributesMap() != null) {
                        for (DataAttributes<Document> dataAttributes : dataModel.getAttributesMap().values()) {
                            dataAttributes.transformValue();
                            dataAttributes.applyDefaultValue();
                            if (!dataAttributes.applyValidationRules()) {
                                log.error("Validation failed for attribute: {}", dataAttributes.getAttributeName()); // More detailed logging
                                throw new DataExtractionException("Validation failed for attribute: " + dataAttributes.getAttributeName(),
                                        ErrorType.VALIDATION_FAILED);
                            }
                            dataAttributes.setLastUpdatedDate(Instant.now());
                        }
                    }
                }
                dataModelRepository.saveAll(dataModels.values());
            }
            return dataModels;
        } catch (Exception e) {
            log.error("Error while reading data from database: Type [{}], DatabaseName [{}], TableName [{}], URL [{}], Error: {}",
                    type, databaseName, tableName, url, e.getMessage());
            throw new ReadFromDbExceptions("Error reading from database", ErrorType.READ_FROM_DB_EXCEPTIONS);
        }
    }


    /**
     * Reads data from a file.
     * @param filePath The path of the file to read from
     * @See FileExtractor
     * @return A list of DataModel objects
     * @throws ReadFromDbExceptions if the file path is invalid
     */
    public List<DataModel<Object>> extractDataFromFile(String filePath) throws DataExtractionException {
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("Invalid file path");
        }

        File file = new File(filePath);
        if (!file.exists() || !file.canRead()) {
            throw new DataExtractionException("File does not exist or cannot be read",
                    ErrorType.FILE_NOT_READABLE);
        }

        try {

            List<DataModel<Object>> dataModels = fileExtractor.readData(filePath);
            extract(dataModels);
            dataModelRepository.saveAll(dataModels);
            return dataModels;
        } catch (IOException e) {
            log.error("Error while reading data from file", e);
            throw new DataExtractionException("e.getMessage()",
                    ErrorType.FILE_NOT_READABLE);
        }
    }

    //== private methods ==

    private void extract(List<DataModel<Object>> dataModels) throws DataExtractionException {
        if (dataModels != null && !dataModels.isEmpty()) {
            for (DataModel<Object> dataModel : dataModels) {
                if (dataModel.getAttributesMap() != null) {
                    for (DataAttributes<Object> dataAttributes : dataModel.getAttributesMap().values()) {
                        dataAttributes.transformValue();
                        dataAttributes.applyDefaultValue();
                        if (!dataAttributes.applyValidationRules()) {
                            log.error("Validation failed for attribute: {}", dataAttributes.getAttributeName());
                            throw new DataExtractionException("Validation failed for attribute: " + dataAttributes.getAttributeName(),
                                    ErrorType.VALIDATION_FAILED);
                        }
                        dataAttributes.setLastUpdatedDate(Instant.now());
                    }
                }
            }
        }
    }


}
