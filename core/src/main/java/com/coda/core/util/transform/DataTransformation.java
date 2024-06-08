package com.coda.core.util.transform;

import com.coda.core.entities.DataAttributes;
import com.coda.core.exceptions.TransformationException;
import com.coda.core.util.types.ErrorType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.Optional;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.OptionalDouble;
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

        if ("String".equals(type)) {
            Map<T, Long> valueCountMap = column.stream()
                    .collect(Collectors
                            .groupingBy(DataAttributes::getValue,
                                    Collectors.counting()));

            valueCountMap.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey);

        } else {
            throw new TransformationException("Error: "
                    + "Unsupported data type for categorical values",
                    ErrorType.TRANSFORMATION_FAILED);
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
        if (Number.class.isAssignableFrom(Class.forName(typeClazzName))) {
            OptionalDouble average = column.stream()
                    .filter(attr -> attr.getValue() != null)
                    .mapToDouble(attr -> ((Number) attr.getValue())
                            .doubleValue()).average();

            if (average.isPresent()) {
                if (attribute.getValue() == null) {
                    if (typeClazzName
                            .equals(Integer.class.getName())) {
                        attribute.setValue((T) Integer
                                .valueOf((int) average.getAsDouble()));
                    } else if (typeClazzName
                            .equals(Double.class.getName())) {
                        attribute.setValue((T)
                                Double.valueOf(average.getAsDouble()));
                    } else if (typeClazzName
                            .equals(Float.class.getName())) {
                        attribute.setValue((T)
                                Float.valueOf((float) average.getAsDouble()));
                    } else if (typeClazzName
                            .equals(Long.class.getName())) {
                        attribute.setValue((T)
                                Long.valueOf((long) average.getAsDouble()));
                    }
                }
            } else {
                log.warn("Mean calculation failed due to empty "
                                + "or invalid data for column: {}",
                        attribute.getAttributeName());
            }
        } else {
            log.error("Attempted to calculate numerical mean"
                            + " for non-numerical type: {}",
                    typeClazzName);
            throw new ArithmeticException("Attempt to calculate numerical "
                    + "mean for non-numerical type failed");
        }
    }

    /**
     * Normalizes the data in the column.
     * @param column The list of data attributes of the column.
     * @param attribute The data attribute to be normalized.
     * @param <T> The type of the value in the data attribute.
     * @throws ClassNotFoundException If the class of the type cannot be found.
     */


    @SuppressWarnings("unchecked")
    public <T> void normalizeData(
            final List<DataAttributes<T>> column,
            final DataAttributes<T> attribute)
            throws ClassNotFoundException {

        String typeClazzName = attribute.getTypeClazzName();
        if (Number.class.isAssignableFrom(Class.forName(typeClazzName))) {
            OptionalDouble mean = column.stream()
                    .filter(attr -> attr.getValue() != null)
                    .mapToDouble(attr -> ((Number) attr.getValue())
                            .doubleValue()).average();
            if (mean.isEmpty()) {
                log.warn("Mean calculation failed due to "
                        + "empty or invalid data for column: {}",
                        attribute.getAttributeName());
                return;
            }
            double meanValue = mean.getAsDouble();

            OptionalDouble variance = column.stream()
                    .filter(attr -> attr.getValue() != null)
                    .mapToDouble(attr -> ((Number) attr.getValue())
                            .doubleValue())
                    .map(val -> Math.pow(val - meanValue, 2)).average();
            if (variance.isEmpty()) {
                log.warn("Standard deviation calculation failed due "
                        + "to empty or invalid data for column: {}",
                        attribute.getAttributeName());
                return;
            }

            double stdDev = Math.sqrt(variance.getAsDouble());

            column.stream()
                    .filter(attr ->
                            attr.getValue() != null)
                    .forEach(attr -> {
                double normalizedValue
                        = ((Number) attr.getValue())
                        .doubleValue() - meanValue;
                normalizedValue /= stdDev;
                try {
                    attr.setValue((T) Class.forName(typeClazzName)
                            .cast(normalizedValue));
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            log.error("Attempted to normalize data for non-numerical type: {}",
                    typeClazzName);
            throw new ArithmeticException("Attempt to normalize data"
                    + " for non-numerical type failed");
        }
    }

    /**
     * encodeCategoricalData encodes categorical data.
     * maps, where each map represents a category
     * and its corresponding value.
     * @param column The list of data attributes of the column.
     * @return A list of encoded categorical data.
     */

    public List<Map<String, Integer>> encodeCategoricalData(
            final List<DataAttributes<Object>> column) {
        // Collect unique categories
        Set<String> uniqueCategories = column.stream()
                .map(DataAttributes::getValue)
                .filter(Objects::nonNull)
                .map(String.class::cast)
                .collect(Collectors.toSet());

        List<Map<String, Integer>> encodedValues = new ArrayList<>();

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

        return encodedValues;
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





}
