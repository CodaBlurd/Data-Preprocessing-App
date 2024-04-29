package com.coda.core.entities;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * DTO for {@link DataModel}
 * @param <T> The type of the value
 * This class is used to store the data attributes of a column in a table
 *
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter @Setter
@Slf4j
public class DataAttributes<T> {

    private T value;
    private String type;
    private String attributeName;
    private String format;
    private boolean required;
    private T defaultValue;
    private String description;
    private String validationRules;
    private Set<String> parsedRules = new HashSet<>();
    private Map<String, T> metadata;
    private Instant lastUpdatedDate;
    private Class<T> typeClazz;  // Class token for type safety

    public DataAttributes(String columnName, Object object, String columnTypeName, Class<T> typeClazz) {
        this.attributeName = columnName;
        this.value = typeClazz.cast(object); // Safe cast
        this.type = columnTypeName;
        this.typeClazz = typeClazz;
        parseValidationRules(); // Parse validation rules in constructor
    }

    private void parseValidationRules() {
        if (validationRules != null && !validationRules.isEmpty()) {
            String[] rules = validationRules.split(",");
            for (String rule : rules) {
                parsedRules.add(rule.trim());
            }
        }
    }

    /**
     * This method is used to transform the value of the attribute
     * @return T the transformed value of type T of the attribute
     */

    public T transformValue() {
        try {
            Object result = null;
            switch (type) {
                case "String":
                    result = String.valueOf(value);
                    break;
                case "Integer":
                    result = Integer.valueOf(String.valueOf(value));
                    break;
                case "Double":
                    result = Double.valueOf(String.valueOf(value));
                    break;
                case "Boolean":
                    result = Boolean.valueOf(String.valueOf(value));
                    break;
                case "Long":
                    result = Long.valueOf(String.valueOf(value));
                    break;
                case "Float":
                    result = Float.valueOf(String.valueOf(value));
                    break;
                case "Date":
                    if (format != null && !format.isEmpty()) {
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
                        result = LocalDateTime.parse(String.valueOf(value), formatter);
                    } else {
                        result = Instant.parse(String.valueOf(value));
                    }
                    break;
                default:
                    result = value;
            }
            return typeClazz.cast(result);  // Type-safe cast
        } catch (ClassCastException | DateTimeParseException | NumberFormatException e) {
            log.error("Error transforming value for {}: {}", attributeName, e.getMessage());
            return value;  // Optionally, could also return null or throw a custom exception
        }
    }

    //== Apply default value if value is null ==
    public void applyDefaultValue() {
        if (value == null) {
            value = defaultValue;
        }
    }

    //== Apply validation rules ==
    /**
     * This method is used to apply the validation rules to the attribute
     * @return boolean true if the validation rules are applied successfully, false otherwise
     */
    public boolean applyValidationRules() {
        // Implement validation rules here
        if (required && value == null) {
            return false;
        }
        if (parsedRules.contains("non-negative") && value instanceof Number && ((Number) value).doubleValue() < 0) {
            return false;
        }
        if (parsedRules.contains("non-empty") && value instanceof String && ((String) value).isEmpty()) {
            return false;
        }
        return true;
    }

    public void setValidationRules(String validationRules) {
        this.validationRules = validationRules;
        parseValidationRules();
    }


}
