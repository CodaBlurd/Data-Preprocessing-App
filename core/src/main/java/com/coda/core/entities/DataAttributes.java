
package com.coda.core.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Objects;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.function.Predicate;

/**
 * DTO for {@link DataModel}.
 * @param <T> The type of the value.
 *<p>This class is used to store the data .
 * attributes of a column in a table.
 * </p>
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter @Setter
@Slf4j
public final class DataAttributes<T> implements Serializable {

    /**
     * The versionID for object serialization.
     */
    @Serial
    private static final long serialVersionUID = 1L;
    /**
     * The list of validation rules to apply to the attribute.
     * <p>
     *     predicates are used to validate the attribute. i.e.
     *     if the attribute is required, non-negative, non-empty etc.
     * </p>
     */
    private List<Predicate<T>> validationRulesList = new ArrayList<>();

    /**
     * The value of the attribute.
     * i.e. the data in the column.
     */
    private T value;

    /**
     * The type of the attribute.
     * i.e. the data type of the column.
     */
    private String type;

    /**
     * The name of the attribute.
     * i.e. the name of the column.
     */
    private String attributeName;

    /**
     * The format of the attribute.
     * i.e. the format to be used for the column.
     */
    private String format;

    /**
     * The required status of the attribute.
     * i.e. whether the column is required or not.
     */
    private boolean required;

    /**
     * The default value of the attribute.
     * i.e. the default value of the column.
     */
    private  T defaultValue;

    /**
     * The description of the attribute.
     * i.e. the purpose of the column.
     */
    private String description;

    /**
     * The validation rules of the attribute.
     * i.e. the rules to be applied to the column.
     */
    private String validationRules;

    /**
     * The parsed rules of the attribute.
     * example of parsed rules are non-negative, non-empty etc.
     */
    private Set<String> parsedRules = new HashSet<>();
    // example Hashset data = { "non-negative", "non-empty"}

    /**
     * The metadata of the attribute.
     * i.e. the data about the column.
     */
    private Map<String, T> metadata = new HashMap<>();
    // example metadata data = {{ "key1" : "value1"}, { "key2" : "value2"}

    /**
     * The last updated date of the attribute.
     * i.e. the date the column was last updated.
     */
    private Instant lastUpdatedDate;

    /**
     * The type class of the attribute.
     * i.e. the class token for type safety.
     */
    private String typeClazzName;

    /**
     * The encoded values of the categorical variables.
     */
    private Map<String, Integer> encodedValues;

    /**
     * Constructor for DataAttributes.
     * @param columnName the name of the column.
     * @param object the object to store.
     * @param columnTypeName the type of the column.
     * @param clazzToken the class token for type safety.
     */
    public DataAttributes(final String columnName,
                          final Object object,
                          final String columnTypeName,
                          final Class<T> clazzToken) {

        this.attributeName = columnName;

        // Safe cast, already checked type
        setValue(object, clazzToken);

        this.type = columnTypeName;

        this.typeClazzName = clazzToken.getName();
        // Parse the validation rules
        parseValidationRules();

        // Initialize the validation rules
        initializeValidationRules();
    }

    private void setValue(final Object object, final Class<T> clazzToken) {
        if (object != null) {
            try {
                this.value = clazzToken.cast(object);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("Object cannot be cast "
                        + "to specified class type "
                        + clazzToken.getName(), e);
            }
        } else {
            this.value = null;
        }
    }

    private void parseValidationRules() {
        if (validationRules != null
                && !validationRules.isEmpty()) {
            Arrays.stream(
                    validationRules.split("\\|"))
                    .map(String::trim).forEach(parsedRules::add);
        }
    }

    /**
     * Applies the default value to the attribute.
     */
    public void applyDefaultValue() {
        if (value == null || (value instanceof String
                && ((String) value).isEmpty())) {
            value = defaultValue;
        }
    }

    /**
     * Initializes the validation rules.
     */
    public void initializeValidationRules() {
        if (required) {
            validationRulesList.add(Objects::nonNull);
        }
        if (parsedRules.contains("non-negative")
                && (value instanceof  Number)) {
            validationRulesList
                    .add(val -> ((Number) val).doubleValue() >= 0);
        }
        if (parsedRules.contains("non-empty")
                && value instanceof  String) {
            validationRulesList
                    .add(val -> !((String) val).isEmpty());
        }

    }

    /**
     * Applies the validation rules to the attribute.
     * @return boolean whether the validation rules are applied or not
     */
    public boolean applyValidationRules() {
        if (value == null
                || (value instanceof String
                && ((String) value).isEmpty())) {
            if (required) {
                log.error("Validation failed for attribute '{}': "
                        + "value is required but null or empty", attributeName);
                return false;
            }
            return true;

        }
        for (Predicate<T> rule : validationRulesList) {
            if (!rule.test(value)) {
                log.error("Validation failed for attribute '{}': "
                        + "rule '{}' not satisfied for value '{}'", attributeName, rule, value);
                return false;
            }
        }

        return true;
    }

    /**
     * Sets the validation rules.
     * @param rules the validation rules to set.
     */
    public void setValidationRules(final String rules) {
        this.validationRules = rules;
        parsedRules.clear();
        parseValidationRules();
        initializeValidationRules();
    }


    @Override
    public String toString() {
        return new StringJoiner(", ",
                DataAttributes.class.getSimpleName() + "[", "]")
                .add("validationRulesList=" + validationRulesList)
                .add("value=" + value)
                .add("type='" + type + "'")
                .add("attributeName='" + attributeName + "'")
                .add("format='" + format + "'")
                .add("required=" + required)
                .add("defaultValue=" + defaultValue)
                .add("description='" + description + "'")
                .add("validationRules='" + validationRules + "'")
                .add("parsedRules=" + parsedRules)
                .add("metadata=" + metadata)
                .add("lastUpdatedDate=" + lastUpdatedDate)
                .add("typeClazzName='" + typeClazzName + "'")
                .toString();
    }
}

