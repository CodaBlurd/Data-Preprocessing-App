package com.coda.core.util.db;

import com.coda.core.batch.extraction.MySQLReader;
import com.coda.core.entities.DataModel;
import com.coda.core.util.Constants;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ReaderFactory {

    /**
     * MySQL reader instance.
     */
    private final MySQLReader mySQLReader;

    /**
     * Data source type.
     */

    @Value(" ${dataSource.type} ")
    private Constants.DataSourceType dataSourceType;

    /**
     * Constructor to inject dependencies.
     *
     * @param reader the MySQL reader
     */

    public ReaderFactory(final MySQLReader reader)  {

        this.mySQLReader = reader;
    }

    /**
     * getReader().
     * This method returns the reader based on the data source type.
     * @return the reader
     */

    public ItemReader<DataModel<Object>> getReader() {
        Constants.DataSourceType type
                = Constants.DataSourceType.valueOf(this.dataSourceType
                .toString().toUpperCase());
        return switch (type) {
            case MYSQL -> mySQLReader;
            default -> throw new IllegalArgumentException("Invalid "
                    + "data source type");
        };

    }
}
