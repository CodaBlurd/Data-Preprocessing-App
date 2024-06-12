package com.coda.core.batch.extraction;

import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataExtractionException;
import com.coda.core.service.DataModelService;
import com.coda.core.util.Constants;
import org.bson.Document;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class DataModelReader implements ItemReader<DataModel<?>> {

    /**
     * The DataModelService Object.
     */
    private final DataModelService dataModelService;

    /**
     * The Index to  iterate over the data.
     */
    private int index = 0;

    /**
     * The DataModel List.
     * @see DataModel
     */
    private List<DataModel<?>> dataModels;

    /**
     * The DataModel document Map.
     */
    private Map<String, DataModel<Document>> documentDataModels;

    /**
     * The DataSourceType Object.
     */
    private final Constants.DataSourceType dataSourceType;

    /**
     * The FilePath String.
     */
    private final String filePath;

    /**
     * The ResourcePath String.
     */
    private final String resourcePath;

    /**
     * The DbType String.
     */
    private final String dbType;

    /**
     * The tableName String.
     */
    private final String tableName;

    /**
     * The DbName String.
     */
    private final String dbName;

    /**
     * The url string.
     */
    private final String url;

    /**
     * Constructor for DataModelReader.
     * @param service the data model service object.
     * @param sourceType the data source type.
     * @param path the file path.
     * @param resPath the resource path.
     * @param type the database type.
     * @param table the table name.
     * @param name the database name.
     * @param dbUrl the database url.
     */

    public DataModelReader(final DataModelService service,
                           @Value("${dataSourceType}")
                           final Constants.DataSourceType sourceType,
                           @Value("${filePath}") final String path,
                           @Value("${resourcePath}") final String resPath,
                           @Value("${dbType}") final String type,
                           @Value("${tableName}") final String table,
                           @Value("${dbName}") final String name,
                           @Value("${url}") final String dbUrl) {
        this.dataModelService = service;
        this.dataSourceType = sourceType;
        this.filePath = path;
        this.resourcePath = resPath;
        this.dbType = type;
        this.tableName = table;
        this.dbName = name;
        this.url = dbUrl;
    }

    /**
     * Reads data from a data source.
     * @return A data model object
     * @throws DataExtractionException If an error occurs during data extraction
     */
    @Override
    @SuppressWarnings("unchecked")
    public DataModel<?> read() throws DataExtractionException {
        if (dataModels == null && documentDataModels == null) {
            switch (dataSourceType) {
                case FILE_SYSTEM:
                    dataModels
                            = (List<DataModel<?>>) (List<?>) dataModelService
                            .extractDataFromFileOnFileSystem(filePath);
                    break;
                case CLASSPATH:
                    dataModels
                            = (List<DataModel<?>>) (List<?>) dataModelService
                            .extractDataFromFile(resourcePath);
                    break;
                case SQL:
                    dataModels
                            = (List<DataModel<?>>) (List<?>) dataModelService
                            .extractDataFromTable(dbType, tableName);
                    break;
                case MONGO:
                    documentDataModels = dataModelService
                            .extractDataFromTable(dbType, dbName,
                                    tableName, url);
                    if (documentDataModels != null
                            && !documentDataModels.isEmpty()) {
                        dataModels
                                = (List<DataModel<?>>)
                                (List<?>) documentDataModels
                                .values().stream()
                                .toList();
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported data "
                            + " source type: " + dataSourceType);
            }
        }

        if (dataModels != null
                && index < dataModels.size()) {
            return dataModels.get(index++);
        }

        return null;
    }
}
