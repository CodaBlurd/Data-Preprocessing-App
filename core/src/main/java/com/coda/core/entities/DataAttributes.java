package com.coda.core.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.Instant;
import java.util.Map;

/**
 * DTO for {@link DataModel}
 * @param <T> The type of the value
 * This class is used to store the data attributes of a column in a table
 *
 */

@NoArgsConstructor
@AllArgsConstructor
@Data
public class DataAttributes<T> {
    private T value;
    private String type;
    private String attributeName;
    private String format;
    private boolean required;
    private T defaultValue;
    private String description;
    private String validationRules;
    private Map<String, T> metadata;
    private Instant lastUpdatedDate;


    public DataAttributes(String columnName, Object object, String columnTypeName) {
        this.attributeName = columnName;
        this.value = (T) object;
        this.type = columnTypeName;
    }
}