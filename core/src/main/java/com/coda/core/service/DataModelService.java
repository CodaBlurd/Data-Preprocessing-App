package com.coda.core.service;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataExtractionException;
import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.db.DatabaseExtractor;
import com.coda.core.util.db.DatabaseExtractorFactory;
import com.coda.core.util.file.FileExtractor;
import com.coda.core.util.types.ErrorType;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service class for DataModel entity.
 * <p> This class is responsible
 * for all the business
 * logic related to DataModel entity
 * </p>
 */

@Service
@Slf4j
public class DataModelService {

    /**
     * The DataModelRepository interface.
     */
    private final DataModelRepository dataModelRepository;

    /**
     * The DatabaseExtractorFactory interface.
     */
    private final DatabaseExtractorFactory databaseExtractorFactory;

    /**
     * The FileExtractor interface.
     */
    private final FileExtractor fileExtractor;

    /**
     * The ResourceLoader interface.
     */

    private final ResourceLoader resourceLoader;

    /**
     * The MongoTemplate interface.
     * This is used to interact with the MongoDB database.
     */
    private final MongoTemplate mongoTemplate;

    /**
     * Constructor for DataModelService.
     * @param dataModels the DataModelRepository object.
     * @param dbExtractorFactory the DatabaseExtractorFactory object.
     * @param fExtractor the FileExtractor object.
     * @param resLoader the ResourceLoader object.
     * @param template the MongoTemplate object.
     */
    @Autowired
    public DataModelService(final DataModelRepository dataModels,
                            final DatabaseExtractorFactory dbExtractorFactory,
                            final FileExtractor fExtractor,
                            final ResourceLoader resLoader,
                            final MongoTemplate template) {
        this.dataModelRepository = dataModels;
        this.databaseExtractorFactory = dbExtractorFactory;
        this.fileExtractor = fExtractor;
        this.resourceLoader = resLoader;
        this.mongoTemplate = template;
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
    public List<DataModel<Object>> extractDataFromTable(
            final String type, final String tableName,
     final String url, final String user, final String password)
            throws ReadFromDbExceptions {

        validateDatabaseArguments(type, tableName, url, user, password);

        try {
            DatabaseExtractor databaseExtractor
                    = databaseExtractorFactory.getExtractor(type);
            List<DataModel<Object>> dataModels = databaseExtractor
                    .readData(tableName, url, user, password);
            processAndSaveDataModels(dataModels);
            return dataModels;

        } catch (Exception e) {
            log.error("Error while reading data from database: {}, Cause: {}",
                    e.getMessage(), "{}", e.getCause());
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
     * @return A map of DataModel objects
     * with the key being the id or key of the document
     * @throws ReadFromDbExceptions if the table name is invalid
     */

    public Map<String, DataModel<Document>> extractDataFromTable(
            final String type, final String databaseName,
            final String tableName, final String url)
            throws ReadFromDbExceptions {

        validateDatabaseArguments(type, tableName, url);

        try {
            DatabaseExtractor databaseExtractor
                    = databaseExtractorFactory.getExtractor(type);
            Map<String, DataModel<Document>> dataModels
                    = databaseExtractor
                    .readData(databaseName, tableName, url);
            if (dataModels != null) {
                processDocumentDataModels(dataModels);
                dataModelRepository
                        .saveAll(dataModels.values());
            }
            return dataModels;
        } catch (Exception e) {
            log.error("Error while reading data from database:"
                            + " Type [{}], DatabaseName [{}], TableName [{}],"
                            + " URL [{}], Error: {}",
                    type, databaseName, tableName,
                    url, e.getMessage());

            throw new ReadFromDbExceptions(
                    "Error reading from database",
                    ErrorType.READ_FROM_DB_EXCEPTIONS);
        }
    }

    /**
     * Reads files from classPath.
     * @param resourcePath The path of the file to read from
     * @return A list of DataModel objects
     * @throws DataExtractionException if the file path is invalid
     */


    public List<DataModel<Object>> extractDataFromFile(
            final String resourcePath)
            throws DataExtractionException {
        try {
            Resource resource = resourceLoader.getResource(
                    "classpath:" + resourcePath);
            if (!resource.exists()) {
                throw new DataExtractionException(
                        "File does not exist or cannot be read",
                        ErrorType.FILE_NOT_READABLE);
            }

            try (InputStream inputStream = resource.getInputStream()) {
                List<DataModel<Object>> dataModels
                        = fileExtractor.readDataWithApacheCSV(inputStream);
                processAndSaveDataModels(dataModels);
                return dataModels;
            }
        } catch (IOException e) {
            log.error("Error while reading data from file", e);
            throw new DataExtractionException(
                    "Error reading from file: "
                    + e.getMessage(), ErrorType.FILE_NOT_READABLE);
        }
    }

    /**
     * Reads data from a file on the file system.
     * @param filePath The path of the file to read from
     * @return A list of DataModel objects
     * @throws DataExtractionException if the file path is invalid
     */

    public List<DataModel<Object>> extractDataFromFileOnFileSystem(
            final String filePath)
            throws DataExtractionException {
        validateFilePath(filePath);

        Path path = Paths.get(filePath);
        validateFileAccess(path);

        try (InputStream inputStream = Files.newInputStream(path)) {
            List<DataModel<Object>> dataModels
                    = fileExtractor.readDataWithApacheCSV(inputStream);
            processAndSaveDataModels(dataModels);
            return dataModels;
        } catch (IOException e) {
            log.error("Error while reading data from file: {}", filePath, e);
            throw new DataExtractionException(
                    "Error reading from file: "
                            + e.getMessage(),
                    ErrorType.FILE_NOT_READABLE);
        } catch (Exception e) {
            log.error("Unexpected error occurred "
                            + "while extracting data from file: {}",
                    filePath, e);
            throw new DataExtractionException("Unexpected error occurred: "
                    + e.getMessage(), ErrorType.UNKNOWN);
        }
    }


    // == Data Load Phase ==

    /**
        * Loads data to a CSV file.
        * @param dataModels The list of DataModel objects to load.
        * @param filePath The path of the file to write to.
        * @throws DataExtractionException if the file path is invalid.
        * @throws IOException if the file cannot be written to.
     */
    public void loadDataToCSV(final List<DataModel<Object>> dataModels,
                              final String filePath)
            throws DataExtractionException, IOException {
        validateLoadArguments(dataModels, filePath);

        try {
            Path path = Paths.get(filePath);
            if (Files.exists(path) && !Files.isWritable(path)) {
                throw new AccessDeniedException("File cannot be written to");
            }

            if (!fileExtractor.canWrite(filePath)) {
                throw new DataExtractionException(
                        "File cannot be written to",
                        ErrorType.FILE_NOT_WRITABLE);
            }

            fileExtractor.writeDataWithApacheCSV(dataModels, filePath);
        } catch (AccessDeniedException e) {
            log.error("Access denied to file: {}", filePath, e);
            throw new DataExtractionException("Access denied to "
                    + "file: " + e.getMessage(),
                    ErrorType.ACCESS_DENIED);
        } catch (IOException e) {
            log.error("Error while writing data to file: {}", filePath, e);
            throw new DataExtractionException(
                    "Error while writing to file: "
                    + e.getMessage(),
                    ErrorType.FILE_NOT_WRITABLE);
        }
    }



    //== private methods ==

    private void validateDatabaseArguments(final String... args) {
        for (String arg : args) {
            if (arg == null || arg.isEmpty()) {
                throw new IllegalArgumentException("Invalid arguments");
            }
        }
    }

    private void validateFilePath(final String filePath)
            throws DataExtractionException {
        if (filePath == null || filePath.isEmpty()) {
            throw new DataExtractionException("Invalid file path",
                    ErrorType.INVALID_FILE_PATH);
        }
    }

    private void validateFileAccess(final Path path)
            throws DataExtractionException {
        if (!Files.exists(path) || !Files.isReadable(path)) {
            throw new DataExtractionException(
                    "File does not exist or cannot be read",
                    ErrorType.FILE_NOT_READABLE);
        }
    }

    private void validateLoadArguments(final List<DataModel<Object>> dataModels,
                                       final String filePath) {
        if (dataModels == null
                || filePath == null
                || filePath.isEmpty()) {
            throw new IllegalArgumentException("Invalid arguments");
        }
    }

    private void processAndSaveDataModels(
            final List<DataModel<Object>> dataModels)
            throws DataExtractionException {
        validateDataModels(dataModels);
        Map<String, List<DataAttributes<Object>>> categorizedAttributes
                = categorizeAttributesByType(dataModels);
        processAttributes(categorizedAttributes);
        saveProcessedDataModels(dataModels);
    }

    private void processDocumentDataModels(
            final Map<String, DataModel<Document>> dataModels)
            throws DataExtractionException {
        for (DataModel<Document>
                dataModel : dataModels.values()) {

            if (dataModel.getAttributesMap() != null) {

                for (DataAttributes<Document> dataAttributes
                        : dataModel.getAttributesMap().values()) {
                    dataAttributes.transformValue();
                    dataAttributes.applyDefaultValue();
                    if (!dataAttributes.applyValidationRules()) {
                        log.error("Validation failed "
                                        + "for attribute with name: {}",
                                dataAttributes.getAttributeName());
                        throw new DataExtractionException(
                                "Validation failed for attribute: "
                                + dataAttributes.getAttributeName(),
                                ErrorType.VALIDATION_FAILED);
                    }
                    dataAttributes.setLastUpdatedDate(Instant.now());
                }
            }
        }
    }

    private void validateDataModels(
            final List<DataModel<Object>> dataModels)
            throws DataExtractionException {
        if (dataModels == null) {
            throw new DataExtractionException(
                    "Data models list cannot be null.",
                    ErrorType.DATA_EXTRACTION_FAILED);
        }
    }



    private Map<String, List<DataAttributes<Object>>>
    categorizeAttributesByType(
            final List<DataModel<Object>> dataModels) {
        Map<String, List<DataAttributes<Object>>> categorizedAttributes
                = new HashMap<>();
        categorizedAttributes.put("numerical", new ArrayList<>());
        categorizedAttributes.put("categorical", new ArrayList<>());

        for (DataModel<Object> dataModel : dataModels) {
            if (dataModel.getAttributesMap() != null) {
                for (DataAttributes<Object>
                        attr : dataModel.getAttributesMap().values()) {
                    categorizeAttribute(categorizedAttributes, attr);
                    attr.transformValue();
                }
            }
        }
        return categorizedAttributes;
    }

    private void categorizeAttribute(final Map<String,
            List<DataAttributes<Object>>> categorizedAttributes,
                                     final DataAttributes<Object> attr) {
        if ("Integer".equals(attr.getType())
                || "Double".equals(attr.getType())) {
            categorizedAttributes.get("numerical").add(attr);
        } else if ("String".equals(attr.getType())) {
            categorizedAttributes.get("categorical").add(attr);
        }
    }

    private void processAttributes(final Map<String,
            List<DataAttributes<Object>>> categorizedAttributes)
            throws DataExtractionException {
        replaceMissingValues(categorizedAttributes);
        applyTransformationsAndValidations(categorizedAttributes);
    }

    private void replaceMissingValues(
            final Map<String, List<DataAttributes<Object>>>
                                              categorizedAttributes) {
        List<DataAttributes<Object>> numericalColumns
                = categorizedAttributes.get("numerical");
        List<DataAttributes<Object>> categoricalColumns
                = categorizedAttributes.get("categorical");

        numericalColumns.forEach(column -> {
            try {
                column.replaceMissingNumericalValues(numericalColumns);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });

        categoricalColumns.forEach(column ->
                column.replaceMissingCategoricalValues(categoricalColumns));
    }

    private void applyTransformationsAndValidations(
            final Map<String, List<DataAttributes<Object>>>
                    categorizedAttributes)
            throws DataExtractionException {
        for (List<DataAttributes<Object>>
                attributes : categorizedAttributes.values()) {
            for (DataAttributes<Object> attr : attributes) {
                attr.applyDefaultValue();
                if (!attr.applyValidationRules()) {
                    log.error("Validation failed for attribute: {}",
                            attr.getAttributeName());
                    throw new DataExtractionException("Validation failed "
                            + "for attribute: "
                            + attr.getAttributeName(),
                            ErrorType.VALIDATION_FAILED);
                }
                attr.setLastUpdatedDate(Instant.now());
            }
        }
    }

    private void saveProcessedDataModels(
            final List<DataModel<Object>> dataModels) {
        dataModelRepository.saveAll(dataModels);
    }

}
