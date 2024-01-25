package com.coda.core.util.file;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;


public class DataParserImpl implements DataParser{

    /**
    * This method is used to parse the data from the file to the data model
    * @param line the line to be parsed
    * @return DataModel<Object>
    */
    public DataModel<Object> parseLine(String line){
        String[] data = line.split(",");
        if (data.length < 2) {
            throw new IllegalArgumentException("Invalid line: " + line);
        }
        String id = data[0];
        Map<String, DataAttributes<Object>> attributes = new HashMap<>();
        for (int i = 1; i < data.length; i++) {
            DataAttributes<Object> attribute = new DataAttributes<>(String.valueOf(i), data[i], "Object");
            attributes.put(String.valueOf(i), attribute);
        }
        DataModel<Object> dataModel = new DataModel<>();
        dataModel.setAttributesMap(attributes);
        return dataModel;

    }

    /**
    * This method is used to parse the data from the file to the data model
    * @param object the object to be parsed
    * @return DataModel<Object>
    */
    public DataModel<Object> parseObject(Object object){
        return null;
    }
}
