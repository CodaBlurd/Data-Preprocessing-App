
package com.coda.core.entities;

import com.coda.core.util.transform.TransformValue;
import com.coda.core.exceptions.TransformationException;
import com.coda.core.util.transform.IntegerTransform;
import com.coda.core.util.transform.LongTransform;
import com.coda.core.util.transform.StringTransform;
import com.coda.core.util.transform.FloatTransform;
import com.coda.core.util.transform.DoubleTransform;
import com.coda.core.util.transform.BooleanTransform;
import com.coda.core.util.transform.LocalDateTimeTransform;
import com.coda.core.util.transform.InstantTransform;
import com.coda.core.util.transform.ObjectTransform;

import com.coda.core.util.types.ErrorType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.StringJoiner;

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

    /**
     * The metadata of the attribute.
     * i.e. the data about the column.
     */
    private Map<String, T> metadata = new HashMap<>();

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
     * The map of transformation strategies.
     * <p>
     *     The map is used to store the transformation strategies
     *     for the different data types.
     * </p>
     */

    private static final Map<String, TransformValue>
            TRANSFORM_VALUE_MAP = Map.of(
            "Integer", new IntegerTransform(),
            "Double", new DoubleTransform(),
            "String", new StringTransform(),
            "LocalDateTime", new LocalDateTimeTransform(),
            "Instant", new InstantTransform(),
            "Boolean", new BooleanTransform(),
            "Long", new LongTransform(),
            "Float", new FloatTransform(),
            "Object", new ObjectTransform()
    );

    /**
     * Constructor for DataAttributes.
     * @param columnName the name of the column.
     * @param object the object to store.
     * @param columnTypeName the type of the column.
     * @param clazzToken the class token for type safety.
     */
    public DataAttributes(
            final String columnName, final Object object,
            final String columnTypeName, final Class<T> clazzToken) {
        this.attributeName = columnName;

        // Safe cast, already checked type
        if (object != null) {

            try {

                this.value = clazzToken.cast(object);
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(
                        "Object cannot be cast to specified class type "
                                + clazzToken.getName(), e);
            }
        } else {
            this.value = null;
        }
        this.type = columnTypeName;

        this.typeClazzName = clazzToken.getName();

        // Parse the validation rules
        parseValidationRules();

        // Initialize the validation rules
        initializeValidationRules();
    }

    private void parseValidationRules() {
        if (validationRules != null
                && !validationRules.isEmpty()) {
            Arrays.stream(
                    validationRules.split(","))
                    .map(String::trim).forEach(parsedRules::add);
        }
    }

    /**
     * Transforms the value based on its type.
     * @return T the transformed value of type T of the attribute
     */
    @SuppressWarnings("unchecked")
    public T transformValue() {
        if (value == null || (value instanceof String
                && ((String) value).isEmpty())) {
            throw new TransformationException(
                    "Transformation failed, value is null or empty",
                    ErrorType.TRANSFORMATION_FAILED);
        }

        TransformValue transformValue = TRANSFORM_VALUE_MAP.get(type);
        if (transformValue == null) {
            log.error("No transformation strategy found for type: {}", type);
            throw new TransformationException(
                    "No transformation strategy found",
                    ErrorType.TRANSFORMATION_STRATEGY_NOT_FOUND);
        }

        try {
            Optional<?> transformedValue
                    = transformValue.transformValue(value.toString(),
                    Class.forName(typeClazzName), format);
            return (T) transformedValue.orElse(null);
        } catch (Exception e) {
            log.error("Transformation failed for attribute '{}', "
                    + "type '{}': {}", attributeName, type, e.getMessage());
            throw new TransformationException(
                    "Error: " + e.getMessage() + " Cause: " + e.getCause(),
                    ErrorType.TRANSFORMATION_FAILED);
        }
    }

    /**
     * Cleans the categorical values.
     */
    @SuppressWarnings("unchecked")
    public void cleanCategoricalValues() throws ClassNotFoundException {
        if ("String".equals(type)) {
            value = (T) Class.forName(typeClazzName)
                    .cast(((String) value)
                            .replaceAll("[^a-zA-Z0-9]", ""));
        }
    }

    /**
     * Replaces missing categorical values
     * with the mode of the column.
     * @param column the list of data attributes of the column.
     */
    public void replaceMissingCategoricalValues(
            final List<DataAttributes<T>> column) {

        if ("String".equals(type)) {
            Map<T, Long> valueCountMap = column.stream()
                    .collect(Collectors
                            .groupingBy(DataAttributes::getValue,
                            Collectors.counting()));

            value = valueCountMap.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse(null);
        }
    }

    /**
     * Replaces missing numerical values
     * with the mean of the column.
     * @param column the list of data attributes of the column.
     */
    @SuppressWarnings("unchecked")
    public void replaceMissingNumericalValues(
            final List<DataAttributes<T>> column)
            throws ClassNotFoundException {

        if (Number.class.isAssignableFrom(Class.forName(typeClazzName))) {
            OptionalDouble average = column.stream()
                    .filter(attr -> attr.getValue() != null)
                    .mapToDouble(attr -> ((Number) attr.getValue())
                            .doubleValue())
                    .average();

            if (average.isPresent()) {
                value = (T) Class.forName(typeClazzName)
                        .cast(average.getAsDouble());
            } else {
                log.warn("Mean calculation failed due to empty "
                        + "or invalid data for column: {}",
                        attributeName);
            }
        } else {
            log.error("Attempted to calculate numerical "
                            + "mean for non-numerical type: {}",
                    typeClazzName);
            throw new ArithmeticException("Attempt to calculate numerical mean "
                    + "for non-numerical type failed");
        }
    }






    /**
     * Applies the default value to the attribute.
     */
    public void applyDefaultValue() {
        value = defaultValue;

    }

    /**
     * Initializes the validation rules.
     */
    public void initializeValidationRules() {
        if (required) {
            validationRulesList.add(Objects::nonNull);
        }
        if (parsedRules.contains("non-negative")
                && Number.class.isAssignableFrom(value.getClass())) {
            validationRulesList
                    .add(val -> ((Number) val).doubleValue() >= 0);
        }
        if (parsedRules.contains("non-empty")
                && value instanceof String) {
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
            return false;
        }

        return validationRulesList.stream()
                .allMatch(rule -> rule.test(value));
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

