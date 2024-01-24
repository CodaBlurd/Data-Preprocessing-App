package com.coda.core.entities;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;

/**
 * Data model class
 * <p> This class is responsible for holding the data set model to be cleaned</p>
 * The data model class will hold the data set model to be cleaned
 * This class is a dynamic class that will be generated based on the data set model
 * {@code @Entity} annotation to mark the class as an entity class
 * {@code @Table} annotation to specify the table name
 * {@code @Id} annotation to specify the primary key
 * {@code @GeneratedValue} annotation to specify the generation strategy
 *
 *
 */

@Slf4j
@Document(collection = "data_model")
@Data
public class DataModel<T> {
//== fields ==
    @Id
    private String id;

    private Map<String, DataAttributes<T>> attributesMap;

    public DataModel(String id, Map<String, DataAttributes<T>> attributes) {
        this.id = id;
        this.attributesMap = attributes;
    }
}


