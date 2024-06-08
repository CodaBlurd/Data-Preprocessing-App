package com.coda.core.service;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataExtractionException;
import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.db.DatabaseExtractor;
import com.coda.core.util.db.DatabaseExtractorFactory;
import com.coda.core.util.file.FileExtractor;
import com.coda.core.util.transform.DataTransformation;
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
     * The DataTransformation object.
     * This is used to transform
     * the data from different sources.
     */

    private final DataTransformation dataTransformation;

    /**
     * Constructor for DataModelService.
     * @param dataModels the DataModelRepository object.
     * @param dbExtractorFactory the DatabaseExtractorFactory object.
     * @param fExtractor the FileExtractor object.
     * @param resLoader the ResourceLoader object.
     * @param template the MongoTemplate object.
     * @param transformation the DataTransformation object.
     */
    @Autowired
    public DataModelService(final DataModelRepository dataModels,
                            final DatabaseExtractorFactory dbExtractorFactory,
                            final FileExtractor fExtractor,
                            final ResourceLoader resLoader,
                            final MongoTemplate template,
                            final DataTransformation transformation) {
        this.dataModelRepository = dataModels;
        this.databaseExtractorFactory = dbExtractorFactory;
        this.fileExtractor = fExtractor;
        this.resourceLoader = resLoader;
        this.mongoTemplate = template;
        this.dataTransformation = transformation;
    }

    //== public methods ==

    //== DATA EXTRACTION PHASE ==

    /**
     * Reads data from a relational database.
     * @param tableName The name of the table to read from
     * @param type The type of the database
     *  The table name must be a valid table name
     * @return A list of DataModel objects
     * @throws ReadFromDbExceptions if the table name is invalid
     */

    @Transactional(rollbackFor = ReadFromDbExceptions.class)
    public List<DataModel<Object>> extractDataFromTable(
            final String type, final String tableName)
            throws ReadFromDbExceptions {

        validateDatabaseArguments(type, tableName);

        try {
            DatabaseExtractor databaseExtractor
                    = databaseExtractorFactory.getExtractor(type);
            List<DataModel<Object>> dataModels = databaseExtractor
                    .readData(tableName);
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
        } catch (IOException | ClassNotFoundException e) {
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
            throws DataExtractionException, ClassNotFoundException {
        validateDataModels(dataModels);
        Map<String, List<DataAttributes<Object>>> categorizedAttributes
                = categorizeAttributesByType(dataModels);
        processAttributes(categorizedAttributes);
        saveProcessedDataModels(dataModels);
    }

    private void processDocumentDataModels(final Map<String,
            DataModel<Document>> dataModels)
            throws DataExtractionException, ClassNotFoundException {

        for (DataModel<Document> dataModel : dataModels.values()) {
            if (dataModel.getAttributesMap() != null) {
                for (DataAttributes<Document> dataAttributes
                        : dataModel.getAttributesMap().values()) {

                    String type = dataAttributes.getType();
                    Object value = dataAttributes.getValue();
                    String typeClazzName = dataAttributes.getTypeClazzName();
                    String format = dataAttributes.getFormat();
                    String attributeName = dataAttributes.getAttributeName();

                    // Apply transformations
                    dataTransformation.transformValue(type, value,
                            typeClazzName, format, attributeName);

                    dataTransformation.cleanCategoricalValues(type, value,
                            typeClazzName);

                    // Handle missing values
                    dataTransformation.replaceMissingCategoricalValues(
                            List.of(dataAttributes), type);

                    //remove outliers
                    List<DataAttributes<Number>> numberAttributes
                            = convertToNumberAttributes(
                                    List.of(dataAttributes));
                    dataTransformation.removeOutliers(numberAttributes);


                    dataTransformation.replaceMissingNumericalValues(
                            List.of(dataAttributes),
                            dataAttributes);

                    // Normalize data
                    dataTransformation
                            .normalizeData(List.of(dataAttributes),
                            dataAttributes);


                    // Apply default values
                    dataAttributes.applyDefaultValue();

                    // Validate attributes
                    if (dataAttributes.applyValidationRules()) {
                        log.error("Validation failed for attribute with name:"
                                + " {}", attributeName);
                        throw new DataExtractionException("Validation failed "
                                + "for attribute: "
                                + attributeName, ErrorType.VALIDATION_FAILED);
                    }

                    // Update last updated date
                    dataAttributes.setLastUpdatedDate(Instant.now());
                }
            }
        }
    }

    // private method to convert List<DataAttributes<Object>>
    // to list of DataAttributes<Number>

    private List<DataAttributes<Number>> convertToNumberAttributes(
            final List<DataAttributes<Document>> attributes) {

        List<DataAttributes<Number>> numberAttributes = new ArrayList<>();
        for (DataAttributes<Document> attribute : attributes) {
            Document document = attribute.getValue();
            if (document != null) {
                // Assuming the Document contains a single Number value
                Number numberValue = document.get("value", Number.class);
                if (numberValue != null) {
                    DataAttributes<Number> numberAttribute
                            = new DataAttributes<>(
                            attribute.getAttributeName(),
                            numberValue,  // Extracted Number value
                            attribute.getType(),
                            Number.class);
                    numberAttributes.add(numberAttribute);
                }
            }
        }
        return numberAttributes;
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
                    String attributeType = attr.getType();

                    categorizedAttributes.computeIfAbsent(attributeType,
                            k -> new ArrayList<>()).add(attr);
                }
            }
        }
        return categorizedAttributes;
    }

    private void processAttributes(final Map<String,
            List<DataAttributes<Object>>> categorizedAttributes)
            throws DataExtractionException, ClassNotFoundException {

        for (Map.Entry<String, List<DataAttributes<Object>>>
                entry : categorizedAttributes.entrySet()) {
            String attributeType = entry.getKey();
            List<DataAttributes<Object>> attributes = entry.getValue();
            switch (attributeType) {
                case "numerical":
                    processNumericAttributes(attributes);
                    break;
                case "categorical":
                    processCategoricalAttributes(attributes);
                    break;
                default:
                    log.error("Unknown attribute type: {}", attributeType);
                    throw new DataExtractionException("Unknown attribute type: "
                            + attributeType, ErrorType.UNKNOWN_ATTRIBUTE_TYPE);
            }
        }
    }

    private void processNumericAttributes(
            final List<DataAttributes<Object>> attributes)
            throws DataExtractionException, ClassNotFoundException {

        List<DataAttributes<Number>> numericAttributes
                = attributes.stream()
                .filter(attr -> attr.getValue() instanceof  Number)
                .map(attr -> new DataAttributes<>(
                                attr.getAttributeName(),
                                attr.getValue(),
                                attr.getType(),
                                Number.class))
                .toList();
        // handle outlier.
        dataTransformation.removeOutliers(numericAttributes);


        for (DataAttributes<Object> dataAttributes : attributes) {
            dataTransformation.replaceMissingNumericalValues(
                    List.of(dataAttributes), dataAttributes);
            dataTransformation.normalizeData(
                    List.of(dataAttributes), dataAttributes);

            if (dataAttributes.applyValidationRules()) {
                log.error("Validation failed for numeric "
                                + "attribute with name: {}",
                        dataAttributes.getAttributeName());

                throw new DataExtractionException("Validation failed for "
                        + "numeric attribute: "
                        + dataAttributes.getAttributeName(),
                        ErrorType.VALIDATION_FAILED);
            }
            dataAttributes.setLastUpdatedDate(Instant.now());
        }
    }

    private void processCategoricalAttributes(
            final List<DataAttributes<Object>> attributes)
            throws DataExtractionException, ClassNotFoundException {

        for (DataAttributes<Object> dataAttributes : attributes) {
            String type = dataAttributes.getType();
            Object value = dataAttributes.getValue();
            String typeClazzName = dataAttributes.getTypeClazzName();

            dataTransformation.cleanCategoricalValues(type,
                    value, typeClazzName);
            dataTransformation.replaceMissingCategoricalValues(
                    List.of(dataAttributes), type);
            dataTransformation.encodeCategoricalData(
                    List.of(dataAttributes));

            if (dataAttributes.applyValidationRules()) {
                log.error("Validation failed for categorical"
                                + " attribute with name: {}",
                        dataAttributes.getAttributeName());
                throw new DataExtractionException("Validation failed for "
                        + "categorical attribute: "
                        + dataAttributes.getAttributeName(),
                        ErrorType.VALIDATION_FAILED);
            }
            dataAttributes.setLastUpdatedDate(Instant.now());
        }
    }


    private void saveProcessedDataModels(final List<DataModel<Object>>
                                                 dataModels)
            throws DataExtractionException {
        try {
            dataModelRepository.saveAll(dataModels);
        } catch (Exception e) {
            log.error("Error saving data models to repository", e);
            throw new DataExtractionException("Error saving data models: "
                    + e.getMessage(), ErrorType.DATA_SAVE_ERROR);
        }
    }

}
