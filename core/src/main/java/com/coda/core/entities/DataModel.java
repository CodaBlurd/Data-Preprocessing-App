package com.coda.core.entities;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * DataModel class to hold the data set model.
 * <p> This class is responsible for
 * holding the data set model to be cleaned
 * </p>
 * @param <T> the type of the data set model

 */
@Slf4j
@Document(collection = "dataModel")
@Data
@NoArgsConstructor
public class DataModel<T> implements Serializable {

    /**
     * The versionID for object serialization.
     */
    @Serial
    private static final long serialVersionUID = 1L;
    /**
     * The id of the data model.
     */
    @Id
    private ObjectId id;

    /**
     * The attributes of the data model.
     */
    private Map<String, DataAttributes<T>> attributesMap
            = new HashMap<>();

    /**
     * TimeStamp.
     */

    @CreatedDate
    private Instant createdDate;

    /**
     * TimeStamp.
     */

    @LastModifiedDate
    private Instant updatedAt;



    /**
     * Constructor for DataModel.
     * @param dataModelId the id of the data model.
     * @param attr the attributes of the data model.
     */

    public DataModel(final ObjectId dataModelId,
                     final Map<String, DataAttributes<T>> attr) {
        this.id = dataModelId;
        this.attributesMap = attr != null ? attr : new HashMap<>();
    }

    @Override
    public final String toString() {
        StringJoiner attributesJoiner
                = new StringJoiner(", ",
                "{", "}");

        for (Map.Entry<String, DataAttributes<T>>
                entry: attributesMap.entrySet()) {

            attributesJoiner.add(entry.getKey()
                    + "=" + entry.getValue());

        }

        return "DataModel{"
                + "id="
                + id
                + ", attributesMap="
                + attributesJoiner
                + '}';
    }
}


