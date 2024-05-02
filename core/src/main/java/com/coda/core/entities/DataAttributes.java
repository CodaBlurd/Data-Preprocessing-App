package com.coda.core.entities;

import com.coda.core.util.transform.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * DTO for {@link DataModel}
 * @param <T> The type of the value
 * This class is used to store the data attributes of a column in a table.
 *
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter @Setter
@Slf4j
public class DataAttributes<T> {
    private List<Predicate<T>> validationRulesList = new ArrayList<>();

    private Optional<T> value = Optional.empty();

    private String type;
    private String attributeName;
    private String format;
    private boolean required;
    private T defaultValue;
    private String description;
    private String validationRules;
    private Set<String> parsedRules = new HashSet<>();
    private Map<String, T> metadata; // Metadata for the attribute
    private Instant lastUpdatedDate;
    private Class<T> typeClazz;  // Class token for type safety

    // Constructor initializes value and parses and initializes validation rules.
    public DataAttributes(String columnName, Object object, String columnTypeName, Class<T> typeClazz) {
        this.attributeName = columnName;
        this.value = Optional.ofNullable(typeClazz.cast(object)); // Safe cast
        this.type = columnTypeName;
        this.typeClazz = typeClazz;
        parseValidationRules();
        initializeValidationRules();
    }

    // Parse validation rules from a single string to a set of rules.
    private void parseValidationRules() {
        if (validationRules != null && !validationRules.isEmpty()) {
            Arrays.stream(validationRules.split(",")).map(String::trim).forEach(parsedRules::add);
        }
    }

    // Static map of transformation strategies to be initialized once for all instances.
    private static final Map<String, TransformValue> TRANSFORM_VALUE_MAP = Map.of(
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
    public Optional<T> transformValue() {
        // Check if the value is present to proceed with transformation
        if (value.isEmpty()) {
            return Optional.empty();
        }

        // Retrieve the appropriate transformation strategy from the map based on the 'type'
        TransformValue transformValue = TRANSFORM_VALUE_MAP.get(type);
        if (transformValue == null) {
            log.error("No transformation strategy found for type: {}", type);
            return Optional.empty();
        }

        try {
            // Attempt to transform the value using the retrieved strategy
            return transformValue.transformValue(value.get().toString(), typeClazz, format);
        } catch (Exception e) {
            // Log the exception details and return an empty Optional if the transformation fails
            log.error("Transformation failed for attribute '{}', type '{}': {}", attributeName, type, e.getMessage());
            return Optional.empty();
        }
    }

    //== remove/substitute bad characters from categorical variables ==
    public void cleanCategoricalValues() {
        if (type.equals("String")) {
            value = value.map(val -> (T) val.toString().replaceAll("[^a-zA-Z0-9]", ""));
        }
    }

    //== replace missing categorical values with the mode of the column ==
    public void replaceMissingCategoricalValues(List<DataAttributes<T>> column) {
        if (type.equals("String")) {
            // Calculate the mode of the column
            Map<T, Long> valueCountMap = column.stream()
                    .collect(Collectors.groupingBy(DataAttributes::getValue, Collectors.counting()));
            T mode = valueCountMap.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse(null);
            // Replace missing values with the mode
            value = value.or(() -> Optional.ofNullable(mode));
        }
    }

    //== replace missing numerical values with the mean of the column ==
    public void replaceMissingNumericalValues(List<DataAttributes<T>> column) {
        if (Number.class.isAssignableFrom(typeClazz)) {
            OptionalDouble average = column.stream()
                    .filter(attr -> attr.getValue() != null)
                    .mapToDouble(attr -> ((Number) attr.getValue()).doubleValue())
                    .average();

            if (average.isPresent()) {
                T meanValue = typeClazz.cast(average.getAsDouble()); // Safe cast, already checked type
                value = value.or(() -> Optional.of(meanValue));
            } else {
                log.warn("Mean calculation failed due to empty or invalid data for column: {}", attributeName);
            }
        } else {
            log.error("Attempted to calculate numerical mean for non-numerical type: {}", typeClazz);
        }
    }






    // Applies the default value if the current value is empty.
    public void applyDefaultValue() {
        value = value.or(() -> Optional.ofNullable(defaultValue));
    }

    // Initialize validation rules based on parsed rules.
    public void initializeValidationRules() {
        if (required) {
            validationRulesList.add(Objects::nonNull);
        }
        if (parsedRules.contains("non-negative") && Number.class.isAssignableFrom(typeClazz)) {
            validationRulesList.add(val -> ((Number) val).doubleValue() >= 0);
        }
        if (parsedRules.contains("non-empty") && String.class.equals(typeClazz)) {
            validationRulesList.add(val -> !((String) val).isEmpty());
        }
    }

    // Applies all validation rules to the current value.
    public boolean applyValidationRules() {
        return value.map(v -> validationRulesList.stream().allMatch(rule -> rule.test(v))).orElse(false);
    }

    // Sets new validation rules and re-initializes the parsed rules.
    public void setValidationRules(String validationRules) {
        this.validationRules = validationRules;
        parsedRules.clear();
        parseValidationRules();
        initializeValidationRules();
    }

    public T getValue() {
        return value.orElse(null);
    }
}
