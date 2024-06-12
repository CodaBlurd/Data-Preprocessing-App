package com.coda.core.service;

import com.coda.core.batch.processor.DataModelProcessor;
import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataExtractionException;
import com.coda.core.exceptions.DataLoadingException;
import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.db.DatabaseExtractor;
import com.coda.core.util.db.DatabaseExtractorFactory;
import com.coda.core.util.file.FileExtractor;
import com.coda.core.util.transform.DataTransformation;
import com.coda.core.util.types.ErrorType;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
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
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.coda.core.util.Constants.BATCH_SIZE;


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
     * The DataModelProcessor object.
     * This is used to process the data
     * before it is saved to the database.
     */
    private  final DataModelProcessor dataModelProcessor;

    /**
     * Constructor for DataModelService.
     * @param dataModels the DataModelRepository object.
     * @param dbExtractorFactory the DatabaseExtractorFactory object.
     * @param fExtractor the FileExtractor object.
     * @param resLoader the ResourceLoader object.
     * @param template the MongoTemplate object.
     * @param transformation the DataTransformation object.
     * @param processor the DataModelProcessor object.
     */
    public DataModelService(final DataModelRepository dataModels,
                            final DatabaseExtractorFactory dbExtractorFactory,
                            final FileExtractor fExtractor,
                            final ResourceLoader resLoader,
                            final MongoTemplate template,
                            final DataTransformation transformation,
                            final DataModelProcessor processor) {
        this.dataModelRepository = dataModels;
        this.databaseExtractorFactory = dbExtractorFactory;
        this.fileExtractor = fExtractor;
        this.resourceLoader = resLoader;
        this.mongoTemplate = template;
        this.dataTransformation = transformation;
        this.dataModelProcessor = processor;
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

        validateArguments(type, tableName);

        try {
            DatabaseExtractor databaseExtractor
                    = databaseExtractorFactory.getExtractor(type.trim()
                    .toLowerCase());
            Objects.requireNonNull(databaseExtractor,
                    "No suitable extractor for provided db type found");
            List<DataModel<Object>> allDataModels = new ArrayList<>();
            int offSet = 0;
            while (true) {
                List<DataModel<Object>> dataModels = databaseExtractor
                        .readData(tableName, BATCH_SIZE, offSet);

                if (dataModels.isEmpty()) {
                    break;
                }

                processAndSaveDataModels(dataModels);
                allDataModels.addAll(dataModels);
                offSet += BATCH_SIZE;
            }
//            List<DataModel<Object>> dataModels = databaseExtractor
//                    .readData(tableName);
//            processAndSaveDataModels(allDataModels);
            return allDataModels;

        } catch (IOException e) {
            log.error("Error while reading data from database: {}, Cause: {}",
                    e.getMessage(), "{}", e.getCause());
            throw new ReadFromDbExceptions("Error reading from database: "
                    + e.getMessage(), ErrorType.READ_FROM_DB_EXCEPTIONS);
        } catch (Exception e) {
            throw new RuntimeException(e);
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

        validateArguments(type, tableName, url);

        try {
            DatabaseExtractor databaseExtractor
                    = databaseExtractorFactory.getExtractor(
                            type.trim().toLowerCase());

            Objects.requireNonNull(databaseExtractor,
                    "No suitable extractor for provided db type found");
            Map<String, DataModel<Document>> dataModels
                    = databaseExtractor
                    .readData(databaseName, tableName, url);
            if (dataModels != null) {
                processDocumentDataModels(dataModels);
                dataModelRepository
                        .saveAll(dataModels.values());
            }
            return dataModels;

        } catch (IOException | ClassNotFoundException e) {
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
     * Loads data to a relational database.
     * @param dataModels the data models list.
     * @param tableName the table name.
     * @param type the database type.
     */

    public void loadDataToSQL(final List<DataModel<Object>> dataModels,
                              final String tableName, final String type) {

        validateArguments(dataModels, tableName, type);
        try {
            DatabaseExtractor extractor
                    = databaseExtractorFactory.getExtractor(
                            type.trim().toLowerCase());

            Objects.requireNonNull(extractor,
                    "No suitable extractor for provided db type found");

            List<List<DataModel<Object>>> partitions
                    = partitionList(dataModels, BATCH_SIZE);
            for (List<DataModel<Object>> batch : partitions) {
                extractor.loadData(batch, tableName);
                dataModelRepository.saveAll(batch);
            }
        } catch (SQLException e) {
            log.error("Error loading data to SQL", e);
            throw new RuntimeException("Error loading data to SQL", e);
        } catch (Exception e) {
            throw new DataLoadingException("Unable to load data to target dest."
                    + e.getMessage(), ErrorType.DATA_LOADING_EXCEPTION);
        }
    }

    /**
     * Loads data to a non-relational database.
     * @param dataModels The list of DataModel objects to load.
     * @param dbName The name of the database to load to.
     * @param tableName The name of the table to load to.
     * @param url The url of the database.
     * @param type The type of the database.
     */


    public void loadDataToMongo(final Map<String,
                                DataModel<Document>> dataModels,
                                final String dbName, final String tableName,
                                final String url, final String type) {

                validateArguments(dataModels, dbName,
                tableName, url, type);

        try {
            DatabaseExtractor extractor
                    = databaseExtractorFactory.getExtractor(type.trim()
                    .toLowerCase());

            Objects.requireNonNull(extractor,
                    "No suitable extractor for provided db type found");

            extractor.loadData(dataModels, dbName, tableName, url);

            dataModelRepository.saveAll(dataModels.values());

        } catch (Exception e) {
            log.error("Error loading data to MongoDB", e);
            throw new DataLoadingException("Error loading data to MongoDB: "
                    + e.getMessage(), ErrorType.DATA_LOADING_EXCEPTION);
        }

    }

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
        validateArguments(dataModels, filePath);

        try {
            Path path = Paths.get(filePath);
            if (Files.exists(path) && !Files.isWritable(path)) {
                throw new AccessDeniedException("File cannot be written to");
            }

            if (!fileExtractor.canWrite(filePath)) {
                throw new DataLoadingException(
                        "File cannot be written to",
                        ErrorType.FILE_NOT_WRITABLE);
            }

            fileExtractor.writeDataWithApacheCSV(dataModels, filePath);
            dataModelRepository.saveAll(dataModels);
        } catch (AccessDeniedException e) {
            log.error("Access denied to file: {}", filePath, e);
            throw new DataLoadingException("Access denied to "
                    + "file: " + e.getMessage(),
                    ErrorType.ACCESS_DENIED);
        } catch (IOException e) {
            log.error("Error while writing data to file: {}", filePath, e);
            throw new DataLoadingException(
                    "Error while writing to file: "
                    + e.getMessage(),
                    ErrorType.FILE_NOT_WRITABLE);
        }
    }





    //== private methods ==

    private void validateArguments(final Object... args) {
        for (Object arg : args) {
            if (arg == null
                    || (arg instanceof String
                    && ((String) arg).isEmpty())) {
                throw new IllegalArgumentException("Invalid argument: " + arg);
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

    private void processAndSaveDataModels(
            final List<DataModel<Object>> dataModels)
            throws DataExtractionException, ClassNotFoundException {

        dataModelProcessor.processAndSaveDataModels(dataModels,
                BATCH_SIZE, dataModelRepository);
    }
    private void processDocumentDataModels(
            final Map<String, DataModel<Document>> dataModels)
            throws DataExtractionException, ClassNotFoundException {

        for (DataModel<Document> dataModel : dataModels.values()) {
            if (dataModel.getAttributesMap() != null) {
                for (DataAttributes<Document> dataAttributes
                        : dataModel.getAttributesMap().values()) {
                    // Process and transform attributes
                    String type = dataAttributes.getType();
                    Object value = dataAttributes.getValue();
                    String typeClazzName = dataAttributes.getTypeClazzName();
                    String format = dataAttributes.getFormat();
                    String attributeName = dataAttributes.getAttributeName();

                    // transform value
                    dataTransformation.transformValue(type, value,
                            typeClazzName, format, attributeName);

                    // clean categorical values
                    dataTransformation.cleanCategoricalValues(type, value,
                            typeClazzName);

                    // replace missing categorical values
                    dataTransformation.replaceMissingCategoricalValues(
                            List.of(dataAttributes), type);

                    // con vert to numberAttributes
                    List<DataAttributes<Number>> numberAttributes
                            = convertToNumberAttributes(
                                    List.of(dataAttributes));

                    // Remove outliers
                    dataTransformation.removeOutliers(numberAttributes);

                    // replace missing numerical values
                    dataTransformation.replaceMissingNumericalValues(
                            List.of(dataAttributes), dataAttributes);

                    // normalize data
                    dataTransformation.normalizeData(
                            List.of(dataAttributes), dataAttributes);

                    // Apply default Value
                    dataAttributes.applyDefaultValue();

                    if (dataAttributes.applyValidationRules()) {
                        throw new DataExtractionException("Validation failed"
                                + " for attribute: " + attributeName,
                                ErrorType.VALIDATION_FAILED);
                    }
                    dataAttributes.setLastUpdatedDate(Instant.now());
                }
            }
        }
    }

    private List<DataAttributes<Number>> convertToNumberAttributes(
            final List<DataAttributes<Document>> attributes) {
        List<DataAttributes<Number>> numberAttributes = new ArrayList<>();

        for (DataAttributes<Document> attribute : attributes) {
            Document document = attribute.getValue();
            if (document != null) {
                Number numberValue = document.get("value", Number.class);
                if (numberValue != null) {
                    DataAttributes<Number> numberAttribute
                            = new DataAttributes<>(attribute.getAttributeName(),
                            numberValue, attribute.getType(), Number.class);
                    numberAttributes.add(numberAttribute);
                }
            }
        }
        return numberAttributes;
    }

    private <T> List<List<T>> partitionList(
            final List<T> list,
            final int size) {

        List<List<T>> partitions = new ArrayList<>();

        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }

}
