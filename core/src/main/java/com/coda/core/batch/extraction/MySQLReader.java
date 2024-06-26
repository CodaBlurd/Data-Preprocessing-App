package com.coda.core.batch.extraction;

import com.coda.core.entities.DataModel;
import com.coda.core.service.DataModelService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Iterator;
import java.util.List;

@Component
@Slf4j
public class MySQLReader implements ItemReader<DataModel<Object>> {

    /**
     * The DataModelService object.
     * @see DataModelService
     */
    private final DataModelService dataModelService;

    /**
     * The iterator for the data.
     */
    private Iterator<DataModel<Object>> dataIterator;
    /**
     * Type of the data source.
     */
    @Value("${datasource.type}")
    private String dataSourceType;

    /**
     * The table name.
     */
    @Value("${datasource.table.name}")
    private String tableName;

    /**
     * The constructor.
     * @param service the data model service
     */

    public MySQLReader(final DataModelService service) {
        this.dataModelService = service;
    }

    @PostConstruct
    private void init() {
        try{
            List<DataModel<Object>> dataModels =
                    dataModelService.extractDataFromTable(dataSourceType, tableName);
            dataIterator = dataModels.iterator();
            log.info("Data size: {}", dataModels.size());
            dataIterator.forEachRemaining(dataModel -> log.info("DataModel: {}", dataModel));
        } catch (Exception e) {
            log.error("Failed to read data from MySQL database", e);
            throw e;
        }

    }

    /**
     * Reads the data from the MySQL database.
     *
     * @return the data model
     */

    @Override
    public DataModel<Object> read() {
        if (dataIterator != null && dataIterator.hasNext()) {
            return dataIterator.next();
        }
        return null;


    }
}
