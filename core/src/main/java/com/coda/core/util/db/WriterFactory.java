package com.coda.core.util.db;

import com.coda.core.batch.load.MySQLWriter;
import com.coda.core.entities.DataModel;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class WriterFactory {

    /**
     * The MySQLWriter instance.
     */
    private final MySQLWriter mySQLWriter;

    /**
     * Constructor.
     * @param writer The MySQLWriter instance.
     */
    public WriterFactory(final MySQLWriter writer) {
        this.mySQLWriter = writer;
    }

    /**
     * Data source type.
     */

    @Value("${dataSource.type}")
    private String dataSourceType;

    /**
     * Get the writer based on the data source type.
     * @return The writer instance.
     * @throws IllegalArgumentException If the data source type is invalid.
     */

    public ItemWriter<DataModel<Object>> getWriter() {
        return switch (dataSourceType.toUpperCase()) {
            case "MYSQL" -> mySQLWriter;
            default -> throw new IllegalArgumentException("Invalid "
                    + "data source type");
        };
    }
}
