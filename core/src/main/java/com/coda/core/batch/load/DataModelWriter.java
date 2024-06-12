package com.coda.core.batch.load;

import com.coda.core.entities.DataModel;
import com.coda.core.service.DataModelService;
import com.coda.core.util.Constants;
import org.bson.Document;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class DataModelWriter implements ItemWriter<DataModel<?>> {

    /**
     * The DataModelService Object.
     */
    private final DataModelService dataModelService;

    /**
     * The DataSourceType Object.
     */
    private final Constants.DataSourceType dataSourceType;

    /**
     * The FilePath.
     */
    private final String filePath;

    private final String resourcePath;

    /**
     * The DbType.
     */
    private final String dbType;

    /**
     * The TableName.
     */
    private final String tableName;

    /**
     * The DbName.
     */
    private final String dbName;

    /**
     * The Url.
     */
    private final String url;


    /**
     * constructor().
     * @param service the dataModelService.
     * @param sourceType the dataSourceType.
     * @param path the filePath.
     * @param type the dbType.
     * @param table the tableName.
     * @param db the dbName.
     * @param dbUrl the url.
     */
    public DataModelWriter(final DataModelService service,
                           @Value("${dataSourceType}")
                           final Constants.DataSourceType sourceType,
                           @Value("${filePath}") final String path,
                           @Value("${resourcePath}") final String resPath,
                           @Value("${dbType}") final String type,
                           @Value("${tableName}") final String table,
                           @Value("${dbName}") final String db,
                           @Value("${url}") final String dbUrl) {
        this.dataModelService = service;
        this.dataSourceType = sourceType;
        this.filePath = path;
        this.resourcePath = resPath;
        this.dbType = type;
        this.tableName = table;
        this.dbName = db;
        this.url = dbUrl;
    }

    /**
     * write().
     * @param chunk the chunk.
     * @throws Exception the exception.
     */

    @Override
    public void write(@NonNull final Chunk<? extends DataModel<?>> chunk)
            throws Exception {

        List<? extends DataModel<?>> items = chunk.getItems();

        switch (dataSourceType) {
            case FILE_SYSTEM:
                dataModelService.loadDataToCSV(
                        (List<DataModel<Object>>) items,
                        filePath);
                break;
            case SQL:
                dataModelService.loadDataToSQL(
                        (List<DataModel<Object>>) items,
                        tableName, dbType);
                break;
            case MONGO:
                dataModelService.loadDataToMongo(
                        (Map<String, DataModel<Document>>) items,
                        dbName, tableName, url, dbType);
                break;
            default:
                throw new IllegalArgumentException("Unsupported"
                        + " data source type: "
                        + dataSourceType);
        }
    }
}
