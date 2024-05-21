package com.coda.core.entities;
import com.coda.core.exceptions.TransformationException;
import com.coda.core.util.transform.DoubleTransform;
import com.coda.core.util.transform.IntegerTransform;
import com.coda.core.util.transform.StringTransform;
import com.coda.core.util.transform.LocalDateTimeTransform;
import com.coda.core.util.transform.InstantTransform;
import com.coda.core.util.transform.BooleanTransform;
import com.coda.core.util.transform.LongTransform;
import com.coda.core.util.transform.FloatTransform;
import com.coda.core.util.transform.ObjectTransform;
import java.util.Objects;
import java.util.OptionalDouble;
import com.coda.core.util.transform.TransformValue;
import com.coda.core.util.types.ErrorType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * DTO for {@link DataModel}.
 * @param <T> The type of the value.
 *<p>This class is used to store the data .
 * attributes of a column in a table.
 * </p>
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Slf4j
public final class DataAttributes<T> {
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
    @Getter
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
    private T defaultValue;

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
    private Map<String, T> metadata;

    /**
     * The last updated date of the attribute.
     * i.e. the date the column was last updated.
     */
    private Instant lastUpdatedDate;

    /**
     * The type class of the attribute.
     * i.e. the class token for type safety.
     */
    private Class<T> typeClazz;

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

        this.typeClazz = clazzToken;

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
     * Transforms the value based on its type.
     * @return T the transformed value of type T of the attribute
     */
    public T transformValue() {

        if (value == null || value instanceof String
                && ((String) value).isEmpty()) {

            throw new TransformationException(
                    "Transformation failed, value is null or empty",
                    ErrorType.TRANSFORMATION_FAILED);

        }

        // Retrieve the appropriate
        // transformation strategy from the map based on the 'type'
        TransformValue transformValue
                = TRANSFORM_VALUE_MAP.get(type);
        if (transformValue == null) {
            log.error("No transformation strategy found for type: {}",
                    type);
            throw new TransformationException(
                    "No transformation strategy found",
                    ErrorType.TRANSFORMATION_STRATEGY_NOT_FOUND);

        }


        try {
            // Attempt to transform the value using the retrieved strategy
            Optional<T> t = transformValue.transformValue(
                    value.toString(),
                    typeClazz, format);
            return t.orElse(null);
        } catch (Exception e) {
            log.error("Transformation failed for attribute '{}',"
                    + " type '{}': {}", attributeName, type, e.getMessage());
            throw new TransformationException(
                    "Error: " + e.getMessage() + " Cause: "
                    + e.getCause(), ErrorType.TRANSFORMATION_FAILED
            );
        }
    }

    /**
     * Cleans the categorical values.
     */
    public void cleanCategoricalValues() {
        if (type.equals("String")) {
            value.toString()
                    .replaceAll("[^a-zA-Z0-9]", "");
        }
    }

    /**
     * Replaces missing categorical values with the mode of the column.
     * @param column the list of data attributes of the column.
     */
    public void replaceMissingCategoricalValues(
            final List<DataAttributes<T>> column) {
        if (type.equals("String")) {
            // Calculate the mode of the column
            Map<T, Long> valueCountMap
                    = column.stream()
                    .collect(Collectors
                            .groupingBy(DataAttributes::getValue,
                                    Collectors.counting()));

            // Replace missing values with the mode
            value = valueCountMap.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse(null);
        }
    }

    /**
     * Replaces missing numerical values with the mean of the column.
     * @param column the list of data attributes of the column.
     */
    public void replaceMissingNumericalValues(
            final List<DataAttributes<T>> column) {
        if (Number.class.isAssignableFrom(typeClazz)) {
            OptionalDouble average
                    = column.stream()
                    .filter(attr -> attr.getValue() != null)
                    .mapToDouble(attr -> ((Number) attr.getValue())
                            .doubleValue())
                    .average();

            if (average.isPresent()) {
                value = typeClazz.cast(average.getAsDouble());
            } else {
                log.warn("Mean calculation failed due to empty "
                        + "or invalid data for column: {}", attributeName);
            }
        } else {
            log.error("Attempted to calculate numerical"
                    + " mean for non-numerical type: {}", typeClazz);
            throw new ArithmeticException(
                    "Attempt to calculate numerical"
                            + " mean for non numerical type: {} failed ");
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
                && Number.class.isAssignableFrom(typeClazz)) {
            validationRulesList.add(val -> ((Number) val)
                    .doubleValue() >= 0);
        }
        if (parsedRules.contains("non-empty")
                && String.class.equals(typeClazz)) {
            validationRulesList.add(val -> !((String) val).isEmpty());
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

}

