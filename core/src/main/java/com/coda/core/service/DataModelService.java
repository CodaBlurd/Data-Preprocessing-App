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
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
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

    private final ResourceLoader resourceLoader;
    @Autowired
    public DataModelService(DataModelRepository dataModelRepository,
                            DatabaseExtractorFactory databaseExtractorFactory,
                            FileExtractor fileExtractor, ResourceLoader resourceLoader) {
        this.dataModelRepository = dataModelRepository;
        this.databaseExtractorFactory = databaseExtractorFactory;
        this.fileExtractor = fileExtractor;
        this.resourceLoader = resourceLoader;
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

    @Transactional(rollbackFor = ReadFromDbExceptions.class)
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
                                log.error("Validation failed for attribute with name: {}", dataAttributes.getAttributeName()); // More detailed logging
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


    public List<DataModel<Object>> extractDataFromFile(String resourcePath) throws DataExtractionException {
        try {
            Resource resource = resourceLoader.getResource("classpath:" + resourcePath);
            if (!resource.exists()) {
                throw new DataExtractionException("File does not exist or cannot be read",
                        ErrorType.FILE_NOT_READABLE);
            }

            try (InputStream inputStream = resource.getInputStream()) {
                List<DataModel<Object>> dataModels = fileExtractor.readDataWithApacheCSV(inputStream);
                extract(dataModels); // Processing extracted data
                dataModelRepository.saveAll(dataModels);
                return dataModels;
            }
        } catch (IOException e) {
            log.error("Error while reading data from file", e);
            throw new DataExtractionException("Error reading from file: " + e.getMessage(), ErrorType.FILE_NOT_READABLE);
        }
    }

    // == Data Load Phase ==

    /*
     * Data loading to csv file destination
     * @param dataModels List of DataModel objects
     * The list must not be null
     * @param filePath The path of the file to write to
     * The file path must be a valid path
     * @throws DataExtractionException if the file path is invalid
     * @throws IOException if an I/O error occurs
     * @return A boolean value indicating the success of the operation
     */
    public void loadDataToCSV(List<DataModel<Object>> dataModels, String filePath) throws DataExtractionException, IOException {
        if (dataModels == null || filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        // Use FileExtractor to check if the file can be written to
        if (!fileExtractor.canWrite(filePath)) {
            throw new DataExtractionException("File cannot be written to",
                    ErrorType.FILE_NOT_WRITABLE);
        }

        // Write the data to the file
        fileExtractor.writeDataWithApacheCSV(dataModels, filePath);
    }



    //== private methods ==

    private void extract(List<DataModel<Object>> dataModels) throws DataExtractionException {
        validateDataModels(dataModels);
        Map<String, List<DataAttributes<Object>>> categorizedAttributes = categorizeAttributesByType(dataModels);
        processAttributes(categorizedAttributes);
        saveProcessedDataModels(dataModels);
    }

    private void validateDataModels(List<DataModel<Object>> dataModels) throws DataExtractionException {
        if (dataModels == null) {
            throw new DataExtractionException("Data models list cannot be null.", ErrorType.DATA_EXTRACTION_FAILED);
        }
    }

    private Map<String, List<DataAttributes<Object>>> categorizeAttributesByType(List<DataModel<Object>> dataModels) {
        Map<String, List<DataAttributes<Object>>> categorizedAttributes = new HashMap<>();
        categorizedAttributes.put("numerical", new ArrayList<>());
        categorizedAttributes.put("categorical", new ArrayList<>());

        for (DataModel<Object> dataModel : dataModels) {
            if (dataModel.getAttributesMap() != null) {
                for (DataAttributes<Object> attr : dataModel.getAttributesMap().values()) {
                    categorizeAttribute(categorizedAttributes, attr);
                    attr.transformValue();
                }
            }
        }
        return categorizedAttributes;
    }

    private void categorizeAttribute(Map<String, List<DataAttributes<Object>>> categorizedAttributes, DataAttributes<Object> attr) {
        if ("Integer".equals(attr.getType()) || "Double".equals(attr.getType())) {
            categorizedAttributes.get("numerical").add(attr);
        } else if ("String".equals(attr.getType())) {
            categorizedAttributes.get("categorical").add(attr);
        }
    }

    private void processAttributes(Map<String, List<DataAttributes<Object>>> categorizedAttributes) throws DataExtractionException {
        replaceMissingValues(categorizedAttributes);
        applyTransformationsAndValidations(categorizedAttributes);
    }

    private void replaceMissingValues(Map<String, List<DataAttributes<Object>>> categorizedAttributes) {
        List<DataAttributes<Object>> numericalColumns = categorizedAttributes.get("numerical");
        List<DataAttributes<Object>> categoricalColumns = categorizedAttributes.get("categorical");
        numericalColumns.forEach(column -> column.replaceMissingNumericalValues(numericalColumns));
        categoricalColumns.forEach(column -> column.replaceMissingCategoricalValues(categoricalColumns));
    }

    private void applyTransformationsAndValidations(Map<String, List<DataAttributes<Object>>> categorizedAttributes) throws DataExtractionException {
        for (List<DataAttributes<Object>> attributes : categorizedAttributes.values()) {
            for (DataAttributes<Object> attr : attributes) {
                attr.applyDefaultValue();
                if (!attr.applyValidationRules()) {
                    log.error("Validation failed for attribute: {}", attr.getAttributeName());
                    throw new DataExtractionException("Validation failed for attribute: " + attr.getAttributeName(), ErrorType.VALIDATION_FAILED);
                }
                attr.setLastUpdatedDate(Instant.now());
            }
        }
    }

    private void saveProcessedDataModels(List<DataModel<Object>> dataModels) {
        dataModelRepository.saveAll(dataModels);
    }

}
