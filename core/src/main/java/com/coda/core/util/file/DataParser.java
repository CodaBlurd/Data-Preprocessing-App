package com.coda.core.util.file;

import com.coda.core.entities.DataModel;
import org.springframework.stereotype.Component;

/**
 * This interface is used to define the data parser
 * <p> This interface is used to define the data parser for the application </p>
 * Used to parse the data from the file to the data model
 *
 */
@Component
public interface DataParser {

        /**
        * This method is used to parse the data from the file to the data model
        * @param line the line to be parsed
        * @return DataModel<Object>
        */
        DataModel<Object> parseLine(String line);

    /**
     * This method is used to parse the data from the file to the data model
     * @param object the object to be parsed
     * @return DataModel<Object>
     */
    DataModel<Object> parseObject(Object object);
}
