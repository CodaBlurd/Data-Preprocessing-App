package com.coda.core.batch.load;

import com.coda.core.entities.DataModel;
import com.coda.core.service.DataModelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
@Slf4j
public class MySQLWriter implements ItemWriter<DataModel<Object>> {
    /**
     * The DataModelService object.
     */
    private final DataModelService dataModelService;

    /**
     * The data source type.
     */

    @Value("${datasource.type}")
    private String dataSourceType;

    /**
     * The table name.
     */

    @Value("${datasource.table.name}")
    private String tableName;

    /**
     * Constructor to inject dependencies.
     *
     * @param service the data model service
     */
    public MySQLWriter(final DataModelService service) {
        this.dataModelService = service;
    }

    /**
     * Writes the data to the MySQL database.
     *
     * @param chunk the chunk of data to write
     */

    @Override
    public void write(final Chunk<? extends DataModel<Object>> chunk) {
        List<? extends DataModel<Object>> dataModels = chunk.getItems();

        // Log the start of the write operation
        log.info("Writing {} items to MySQL database...", dataModels.size());

        // Log individual items
        dataModels.forEach(dataModel -> log.debug("Writing data: {}", dataModel));

        try {
            // Write the data to the MySQL database
            dataModelService.loadDataToSQL((List<DataModel<Object>>) dataModels,
                    dataSourceType, tableName);
        } catch (Exception e) {
            // Log error if the writing fails
            log.error("Failed to write data to MySQL database", e);
            throw e; // Re-throw the exception to ensure batch step fails
        }

        // Log completion of the write operation
        log.info("Successfully wrote {} items to MySQL database.", dataModels.size());
    }
}
