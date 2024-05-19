package com.coda.core.entities;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.HashMap;
import java.util.Map;

/**
 * DataModel class to hold the data set model.
 * <p> This class is responsible for
 * holding the data set model to be cleaned
 * </p>
 * @param <T> the type of the data set model

 */
@Slf4j
@Document(collection = "data_model")
@Data
@NoArgsConstructor
public class DataModel<T> {
    /**
     * The id of the data model.
     */
    @Id
    private String id;

    /**
     * The attributes of the data model.
     */
    private Map<String, DataAttributes<T>> attributesMap
            = new HashMap<>();

    /**
     * Constructor for DataModel.
     * @param dataModelId the id of the data model.
     * @param attr the attributes of the data model.
     */

    public DataModel(final String dataModelId,
                     final Map<String, DataAttributes<T>> attr) {
        this.id = dataModelId;
        this.attributesMap = attr;
    }
}


