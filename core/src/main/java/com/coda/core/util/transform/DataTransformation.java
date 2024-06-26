package com.coda.core.util.transform;

import com.coda.core.entities.DataAttributes;
import com.coda.core.exceptions.TransformationException;
import com.coda.core.util.types.ErrorType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
@Component
public class DataTransformation {

    /**
     * The map of transformation strategies.
     * <p>
     * The map is used to store the transformation strategies
     * for the different data types.
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
     *
     * @param type          The type of the value.
     * @param value         The value to be transformed.
     * @param typeClazzName The class name of the type.
     * @param format        The format of the value.
     * @param attributeName The attribute name.
     * @param <T>           The type of the transformed value.
     * @return The transformed value of type T.
     */
    @SuppressWarnings("unchecked")
    public <T> T transformValue(final String type, final Object value,
                                final String typeClazzName,
                                final  String format,
                                final String attributeName) {

        if (value == null || (value instanceof String
                && ((String) value).isEmpty())) {
            throw new TransformationException("Transformation failed,"
                    + " value is null or empty",
                    ErrorType.TRANSFORMATION_FAILED);
        }

        TransformValue transformValue
                = TRANSFORM_VALUE_MAP.get(type);
        if (transformValue == null) {
            log.error("No transformation strategy "
                    + "found for type: {}", type);
            throw new TransformationException("No transformation "
                    + "strategy found",
                    ErrorType.TRANSFORMATION_STRATEGY_NOT_FOUND);
        }

        try {
            Optional<?> transformedValue = transformValue
                    .transformValue(value.toString(),
                    Class.forName(typeClazzName), format);

            return (T) transformedValue.orElse(null);

        } catch (Exception e) {
            log.error("Transformation failed for attribute '{}',"
                            + " type '{}': {}",
                    attributeName, type, e.getMessage(), e);
            throw new TransformationException("Error: "
                    + e.getMessage() + " Cause: " + e.getCause(),
                    ErrorType.TRANSFORMATION_FAILED);
        }
    }

    /**
     * Cleans categorical values.
     *
     * @param type          The type of the value.
     * @param value         The value to be cleaned.
     * @param typeClazzName The class name of the type.
     * @param <T>           The type of the cleaned value.
     * @return The cleaned value of type T.
     * @throws ClassNotFoundException If the class of the type cannot be found.
     */
    @SuppressWarnings("unchecked")
    public <T> T cleanCategoricalValues(final String type, final Object value,
                                        final String typeClazzName)
            throws ClassNotFoundException {
        if ("String".equals(type) && value instanceof String) {
            return (T) Class.forName(typeClazzName)
                    .cast(((String) value)
                            .replaceAll("[^a-zA-Z0-9]", ""));
        }
        return (T) value;
    }


    /**
     * Replaces missing categorical values
     * with the mode of the column.
     *
     * @param column The list of data attributes of the column.
     * @param type the type of the value in the data attributes.
     * @param <T>    The type of the value in the data attributes.
     */
    public <T> void replaceMissingCategoricalValues(
            final List<DataAttributes<T>> column,
            final String type) {

        if (column.isEmpty()
                || column.stream()
                .allMatch(tDataAttributes
                        -> tDataAttributes.getValue() == null)) {
            throw new TransformationException("Error: Column is "
                    + "empty or values are null",
                    ErrorType.TRANSFORMATION_FAILED);
        }

        if ("java.lang.String".equals(type)
                || "VARCHAR".equals(type)
                || "TEXT".equals(type)) {
            Map<T, Long> valueCountMap = column.stream()
                    .filter(columnData
                            -> columnData.getValue() != null
                            && !columnData.getValue().toString().isEmpty())
                    .collect(Collectors
                            .groupingBy(DataAttributes::getValue,
                            Collectors.counting()));

            T mostFrequentValue = valueCountMap.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElseThrow(() ->
                            new TransformationException("Error: No "
                                    + "values found",
                            ErrorType.TRANSFORMATION_FAILED));

            for (DataAttributes<T> dataAttribute : column) {
                if (dataAttribute.getValue() == null
                        || dataAttribute.getValue().toString().isEmpty()) {
                    dataAttribute.setValue(mostFrequentValue);
                }
            }
        } else {
            throw new TransformationException("Error: Unsupported data type "
                    + " for categorical values",
                    ErrorType.TRANSFORMATION_FAILED);
        }
    }

    /**
     * Replaces missing categorical values with the mode of the column.
     * @param column The list of data attributes of the column.
     * @param type The type of the value in the data attributes.
     * @param <T> The type of the value in the data attributes.
     */
    public <T> void replaceMissingCategoricalValuesForObject(
            final List<DataAttributes<T>> column,
            final String type) {

        if (column.isEmpty()) {
            throw new TransformationException("Error: Column is empty",
                    ErrorType.TRANSFORMATION_FAILED);
        }
        if ("Object".equals(type)) {
            Map<T, Long> valueCountMap = column.stream()
                    .filter(attr -> attr.getValue() != null
                            && !attr.getValue()
                            .toString().isEmpty()) // Filter out null values
                    .collect(Collectors
                            .groupingBy(DataAttributes::getValue,
                            Collectors.counting()));

            T mostFrequentValue
                    = valueCountMap.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse(null);

            for (DataAttributes<T> dataAttribute : column) {
                if (dataAttribute.getValue() == null
                        || dataAttribute.getValue().toString().isEmpty()) {
                    dataAttribute.setValue(mostFrequentValue);
                }
            }

        }


    }

    /**
     * Replaces missing numerical values with the mean of the column.
     * @param column The list of data attributes of the column.
     * @param attribute The data attribute to be replaced.
     * @param <T> The type of the value in the data attribute.
     * @throws ClassNotFoundException If the class of the type cannot be found.
     */

    @SuppressWarnings("unchecked")
    public <T> void replaceMissingNumericalValues(
            final List<DataAttributes<T>> column,
            final DataAttributes<T> attribute)
            throws ClassNotFoundException {

        String typeClazzName = attribute.getTypeClazzName();
        log.info("Type Class Name: {}", typeClazzName);
        log.info("Attribute Value: {}", attribute.getValue());
        log.info("Attribute Type: {}", attribute.getType());

        Class<?> clazz = Class.forName(typeClazzName);

        if (Number.class.isAssignableFrom(clazz)) {
            OptionalDouble average = column.stream()
                    .filter(attr -> attr.getValue() != null)
                    .mapToDouble(attr -> ((Number) attr.getValue())
                            .doubleValue())
                    .average();
            log.info("Average: {}", average);

            if (average.isPresent()) {
                if (attribute.getValue() == null) {
                    double avgValue = average.getAsDouble();
                    switch (typeClazzName) {
                        case "java.lang.Integer":
                            attribute.setValue((T) Integer
                                    .valueOf((int) avgValue));
                            break;
                        case "java.lang.Double":
                            attribute.setValue((T) Double
                                    .valueOf(avgValue));
                            break;
                        case "java.lang.Float":
                            attribute.setValue((T) Float
                                    .valueOf((float) avgValue));
                            break;
                        case "java.lang.Long":
                            attribute.setValue((T) Long
                                    .valueOf((long) avgValue));
                            break;
                        case "java.math.BigDecimal":
                            attribute.setValue((T) BigDecimal
                                    .valueOf(avgValue));
                            break;
                        default:
                            log.error("Unsupported numerical type: {}",
                                    typeClazzName);
                            throw new IllegalArgumentException("Unsupported "
                                    + "numerical type: " + typeClazzName);
                    }
                }
            } else {
                log.warn("Mean calculation "
                        + "failed due to empty or "
                        + "invalid data for column: {}",
                        attribute.getAttributeName());
            }
        } else {
            log.error("Attempted to calculate numerical "
                    + "mean for non-numerical type: {}",
                    typeClazzName);
            throw new ArithmeticException("Attempt to "
                    + "calculate numerical "
                    + "mean for non-numerical type failed");
        }
    }



    /**
     * Normalizes the data in the column.
     * @param column The list of data attributes of the column.
     * @param attribute The data attribute to be normalized.
     * @param <T> The type of the value in the data attribute.
     * @throws ClassNotFoundException If the class of -
     * the type cannot be found.
     */


    @SuppressWarnings("unchecked")
    public <T> void normalizeData(
            final List<DataAttributes<T>> column,
            final DataAttributes<T> attribute)
            throws ClassNotFoundException {

        String typeClazzName = attribute.getTypeClazzName();
        log.info("Type Class Name: {}", typeClazzName);

        if (Number.class
                .isAssignableFrom(
                        Class.forName(typeClazzName))) {

            OptionalDouble meanOpt
                    = column.stream()
                    .filter(attr -> attr.getValue() != null)
                    .mapToDouble(attr -> ((Number) attr.getValue())
                            .doubleValue())
                    .average();

            if (meanOpt.isEmpty()) {
                log.warn("Mean calculation failed due to empty "
                                + " or invalid data for column: {}",
                        attribute.getAttributeName());
                return;
            }
            double meanValue = meanOpt.getAsDouble();
            log.info("Mean: {}", meanValue);

            OptionalDouble varianceOpt = column.stream()
                    .filter(attr -> attr.getValue() != null)
                    .mapToDouble(attr -> {
                        double val = ((Number)
                                attr.getValue()).doubleValue();
                        return Math.pow(val - meanValue, 2);
                    }).average();

            if (varianceOpt.isEmpty()) {
                log.warn("Standard deviation calculation "
                                + "failed due to empty or "
                                + "invalid data for column: {}",
                        attribute.getAttributeName());
                return;
            }
            double stdDev
                    = Math.sqrt(varianceOpt.getAsDouble());
            log.info("Standard Deviation: {}", stdDev);

            column.stream()
                    .filter(attr -> attr.getValue() != null)
                    .forEach(attr -> {

                        double normalizedValue;
                        if (stdDev == 0) {
                            normalizedValue = 0;
                        } else {
                            normalizedValue = (((Number) attr
                                    .getValue())
                                    .doubleValue() - meanValue) / stdDev;
                        }

                        log.info("Normalized Value: {}", normalizedValue);
                        try {
                            T castedValue = switch (typeClazzName) {
                                case "java.lang.Integer"
                                        -> (T) Integer
                                        .valueOf((int) normalizedValue);
                                case "java.lang.Double"
                                        -> (T) Double
                                        .valueOf(normalizedValue);
                                case "java.lang.Float"
                                        -> (T) Float
                                        .valueOf((float) normalizedValue);
                                case "java.lang.Long"
                                        -> (T) Long
                                        .valueOf((long) normalizedValue);
                                case "java.math.BigDecimal"
                                        -> (T) BigDecimal
                                        .valueOf(normalizedValue);
                                default ->
                                        throw new IllegalArgumentException(
                                                "Unsupported "
                                                + "numerical type: "
                                                + typeClazzName);
                            };
                            attr.setValue(castedValue);
                        } catch (Exception e) {
                            throw new RuntimeException("Error setting "
                                    + "normalized value", e);
                        }
                    });
        } else {
            log.error("Attempted to normalize "
                            + "data for non-numerical type: {}",
                    typeClazzName);
            throw new ArithmeticException("Attempt to normalize "
                    + "data for non-numerical type failed");
        }
    }


    /**
     * encodeCategoricalData encodes categorical data.
     * maps, where each map represents a category
     * and its corresponding value.
     *
     * @param column The list of data attributes of the column.
     */

    public void encodeCategoricalData(
            final List<DataAttributes<Object>> column) {
        // Collect unique categories
        Set<String> uniqueCategories = column.stream()
                .map(DataAttributes::getValue)
                .filter(Objects::nonNull)
                .map(String.class::cast)
                .collect(Collectors.toSet());

        List<Map<String, Integer>> encodedValues
                = new ArrayList<>();

        for (DataAttributes<Object> attribute : column) {
            Map<String, Integer> encodedValue = new HashMap<>();

            for (String category : uniqueCategories) {
                if (attribute.getValue() instanceof String) {
                    encodedValue.put(category,
                            category.equals(attribute.getValue()) ? 1 : 0);
                } else {
                    throw new IllegalArgumentException("Category values must"
                            + " be of type String");
                }
            }
            encodedValues.add(encodedValue);
        }

    }

    /**
     * removeOutliers().
     * This method handles outliers in the data.
     * It removes outliers from the data.
     * @param column The list of data attributes of the column.
     * @param <T> The type of the value in the data attribute.
     *
     */

    public <T extends Number> void removeOutliers(
            final List<DataAttributes<T>> column) {

        //constants for quartile calculations.
        final int firstQuartileFactor = 4;
        final int thirdQuartileFactor = 3;
        final double outlierThresholdFactor = 1.5;
        // Convert column values to a list of doubles and sort
        List<Double> values = column.stream()
                .map(attr -> attr.getValue().doubleValue())
                .sorted()
                .toList();

        // Calculate Q1 and Q3
        double q1 = values.get(values.size() / firstQuartileFactor);
        double q3 = values.get(values.size()
                / firstQuartileFactor * thirdQuartileFactor);

        // Calculate the inter-quartile range (IQR)

        double iqr = q3 - q1;

        // Define the outlier thresholds
        double lowerThreshold = q1 - outlierThresholdFactor * iqr;
        double upperThreshold = q3 + outlierThresholdFactor * iqr;

        // Remove outliers from the column
        column.removeIf(attr -> {
            double value = attr.getValue().doubleValue();
            return value < lowerThreshold || value > upperThreshold;
        });
    }

    /**
     * replaceMissingObjectValues().
     * @param dataAttributes The list of data attributes of the column.
     * @param dataAttributes1 The data attribute to be replaced.
     */

    public void replaceMissingObjectValues(
            final List<DataAttributes<Object>> dataAttributes,
            final DataAttributes<Object> dataAttributes1) {


    }

    //standardize data formats



}
