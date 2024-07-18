package com.coda.core.util.transform;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.TransformationException;
import com.coda.core.util.types.ErrorType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class DataTransformation {

    private static final Map<String, TransformValue> TRANSFORM_VALUE_MAP = Map.ofEntries(
            Map.entry("java.lang.Integer", new IntegerTransform()),
            Map.entry("java.lang.Double", new DoubleTransform()),
            Map.entry("java.lang.Float", new FloatTransform()),
            Map.entry("java.lang.Long", new LongTransform()),
            Map.entry("java.math.BigDecimal", new BigDecimalTransform()),
            Map.entry("java.lang.String", new StringTransform()),
            Map.entry("java.lang.Object", new ObjectTransform()),
            Map.entry("java.lang.Boolean", new BooleanTransform()),
            Map.entry("java.time.LocalDateTime", new LocalDateTimeTransform()),
            Map.entry("java.sql.Timestamp", new LocalDateTimeTransform()),
            Map.entry("java.lang.Instant", new InstantTransform()),
            Map.entry("VARCHAR", new StringTransform()),
            Map.entry("TEXT", new StringTransform())
    );

    public <T> T transformValue(final String type, final Object value,
                                final String format, final String attributeName) {

        if (value == null || (value instanceof String && ((String) value).isEmpty())) {
            throw new TransformationException("Transformation failed, value is null or empty",
                    ErrorType.TRANSFORMATION_FAILED);
        }

        TransformValue transformValue = TRANSFORM_VALUE_MAP.get(type);
        if (transformValue == null) {
            log.error("No transformation strategy found for type: {}", type);
            throw new TransformationException("No transformation strategy found",
                    ErrorType.TRANSFORMATION_STRATEGY_NOT_FOUND);
        }

        try {
            if ("VARCHAR".equals(type) || "TEXT".equals(type)) {
                Optional<?> transformedValue = transformValue.transformValue(value.toString(),
                        String.class, format);
                log.info("Transformed value: {}", transformedValue.orElse(null));
                return (T) transformedValue.orElse(null);
            }
            Optional<?> transformedValue = transformValue.transformValue(value.toString(),
                    Class.forName(type), format);
            log.info("Transformed value: {}", transformedValue.orElse(null));

            return (T) transformedValue.orElse(null);

        } catch (Exception e) {
            log.error("Transformation failed for attribute '{}', type '{}': {}",
                    attributeName, type, e.getMessage(), e);
            throw new TransformationException("Error: " + e + " Cause: " + e.getCause(),
                    ErrorType.TRANSFORMATION_FAILED);
        }
    }

    public <T> T cleanCategoricalValues(final String type, final Object value) throws ClassNotFoundException {
        if ("String".equals(type) && value instanceof String) {
            return (T) Class.forName(type).cast(((String) value).replaceAll("[^a-zA-Z0-9]", ""));
        }
        log.info("Cleaned Value: {}", value);
        return (T) value;
    }

    public <T> void replaceMissingCategoricalValues(final List<DataAttributes<T>> column,
                                                    final String type) {

        if (column.isEmpty() || column.stream().allMatch(tDataAttributes -> tDataAttributes.getValue() == null)) {
            throw new TransformationException("Error: Column is empty or values are null",
                    ErrorType.TRANSFORMATION_FAILED);
        }

        if ("java.lang.String".equals(type) || "VARCHAR".equals(type) || "TEXT".equals(type)) {
            Map<T, Long> valueCountMap = column.stream()
                    .filter(columnData -> columnData.getValue() != null && !columnData.getValue().toString().isEmpty())
                    .collect(Collectors.groupingBy(DataAttributes::getValue, Collectors.counting()));

            T mostFrequentValue = valueCountMap.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElseThrow(() -> new TransformationException("Error: No values found",
                            ErrorType.TRANSFORMATION_FAILED));

            for (DataAttributes<T> dataAttribute : column) {
                if (dataAttribute.getValue() == null || dataAttribute.getValue().toString().isEmpty()) {
                    dataAttribute.setValue(mostFrequentValue);
                }
            }
        } else {
            throw new TransformationException("Error: Unsupported data type for categorical values",
                    ErrorType.TRANSFORMATION_FAILED);
        }
    }

    public <T> void replaceMissingCategoricalValuesForObject(final List<DataAttributes<T>> column,
                                                             final String type) {

        if (column.isEmpty()) {
            throw new TransformationException("Error: Column is empty",
                    ErrorType.TRANSFORMATION_FAILED);
        }
        if ("Object".equals(type)) {
            Map<T, Long> valueCountMap = column.stream()
                    .filter(attr -> attr.getValue() != null && !attr.getValue().toString().isEmpty())
                    .collect(Collectors.groupingBy(DataAttributes::getValue, Collectors.counting()));

            T mostFrequentValue = valueCountMap.entrySet().stream()
                    .max(Map.Entry.comparingByValue())
                    .map(Map.Entry::getKey)
                    .orElse(null);

            for (DataAttributes<T> dataAttribute : column) {
                if (dataAttribute.getValue() == null || dataAttribute.getValue().toString().isEmpty()) {
                    dataAttribute.setValue(mostFrequentValue);
                }
            }

        }
    }

    public <T> void replaceMissingNumericalValues(final List<DataAttributes<T>> column,
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
                    .mapToDouble(attr -> ((Number) attr.getValue()).doubleValue())
                    .average();
            log.info("Average: {}", average);

            if (average.isPresent()) {
                if (attribute.getValue() == null) {
                    double avgValue = average.getAsDouble();
                    Number newValue = convertToType(avgValue, clazz);
                    if (newValue != null) {
                        attribute.setValue((T) clazz.cast(newValue));
                        log.info("Replaced missing value with average: {}", avgValue);
                    } else {
                        log.warn("Conversion to type {} resulted in null", clazz.getName());
                    }
                }
            } else {
                log.warn("Mean calculation failed due to empty or invalid data for column: {}",
                        attribute.getAttributeName());
            }
        } else {
            log.error("Attempted to calculate numerical mean for non-numerical type: {}", typeClazzName);
            throw new ArithmeticException("Attempt to calculate numerical mean for non-numerical type failed");
        }
    }

    private Number convertToType(double avgValue, Class<?> clazz) {
        if (clazz.equals(Integer.class)) {
            return (int) avgValue;
        } else if (clazz.equals(Double.class)) {
            return avgValue;
        } else if (clazz.equals(Float.class)) {
            return (float) avgValue;
        } else if (clazz.equals(Long.class)) {
            return (long) avgValue;
        } else if (clazz.equals(BigDecimal.class)) {
            return BigDecimal.valueOf(avgValue);
        } else {
            throw new IllegalArgumentException("Unsupported numerical type: " + clazz.getName());
        }
    }

    public <T> void normalize(List<DataModel<T>> dataModels) throws ClassNotFoundException {
        Map<String, List<DataAttributes<T>>> attributeGroups = new HashMap<>();

        for (DataModel<T> dataModel : dataModels) {
            for (DataAttributes<T> attribute : dataModel.getAttributesMap().values()) {
                if (Number.class.isAssignableFrom(Class.forName(attribute.getType()))) {
                    attributeGroups.computeIfAbsent(attribute.getAttributeName(), k -> new ArrayList<>()).add(attribute);
                }
            }
        }

        for (Map.Entry<String, List<DataAttributes<T>>> entry : attributeGroups.entrySet()) {
            normalizeData(entry.getValue());
        }
    }

    private <T> void normalizeData(final List<DataAttributes<T>> column)
            throws ClassNotFoundException {
        if (column.isEmpty()) return;

        String type = column.get(0).getType();
        log.info("Type Class Name: {}", type);

        if (Number.class.isAssignableFrom(Class.forName(type))) {
            OptionalDouble meanOpt = column.stream()
                    .filter(attr -> !Objects.equals(attr.getAttributeName(), "id"))
                    .filter(attr -> attr.getValue() != null)
                    .mapToDouble(attr -> ((Number) attr.getValue()).doubleValue())
                    .average();

            if (meanOpt.isEmpty()) {
                log.warn("Mean calculation failed due to empty or invalid data for column: {}",
                        column.get(0).getAttributeName());
                return;
            }
            double meanValue = meanOpt.getAsDouble();
            log.info("Mean: {}", meanValue);

            OptionalDouble varianceOpt = column.stream()
                    .filter(attr -> !Objects.equals(attr.getAttributeName(), "id"))
                    .filter(attr -> attr.getValue() != null)
                    .mapToDouble(attr -> {
                        double val = ((Number) attr.getValue()).doubleValue();
                        return Math.pow(val - meanValue, 2);
                    }).average();

            if (varianceOpt.isEmpty()) {
                log.warn("Standard deviation calculation failed due to empty or invalid data for column: {}",
                        column.get(0).getAttributeName());
                return;
            }
            double variance = varianceOpt.getAsDouble();
            double stdDev = Math.sqrt(variance);
            log.info("Standard Deviation: {}", stdDev);

            if (stdDev == 0) {
                log.warn("Standard deviation is zero, normalization not applicable");
                setNormalizedValueToZero(column, type);
            } else {
                column.stream()
                        .filter(attr -> attr.getValue() != null)
                        .forEach(attr -> {
                            double normalizedValue = (((Number) attr.getValue()).doubleValue() - meanValue) / stdDev;
                            log.info("Original Value: {}, Normalized Value: {}", attr.getValue(), normalizedValue);
                            setNormalizedValue(attr, normalizedValue, type);
                        });
            }
        } else {
            log.error("Attempted to normalize data for non-numerical type: {}", type);
            throw new ArithmeticException("Attempt to normalize data for non-numerical type failed");
        }
    }

    private <T> void setNormalizedValueToZero(List<DataAttributes<T>> column, String type) {
        column.stream()
                .filter(attr -> attr.getValue() != null)
                .forEach(attr -> {
                    try {
                        T castedValue = castToType(type, 0.0);
                        attr.setValue(castedValue);
                    } catch (Exception e) {
                        throw new RuntimeException("Error setting normalized value", e);
                    }
                });
    }

    private <T> void setNormalizedValue(DataAttributes<T> attr, double normalizedValue, String type) {
        try {
            T castedValue = castToType(type, normalizedValue);
            attr.setValue(castedValue);
        } catch (Exception e) {
            throw new RuntimeException("Error setting normalized value", e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T castToType(String type, double value) {
        return switch (type) {
            case "java.lang.Integer" -> (T) Integer.valueOf((int) value);
            case "java.lang.Double" -> (T) Double.valueOf(value);
            case "java.lang.Float" -> (T) Float.valueOf((float) value);
            case "java.lang.Long" -> (T) Long.valueOf((long) value);
            case "java.math.BigDecimal" -> (T) BigDecimal.valueOf(value);
            default -> throw new IllegalArgumentException("Unsupported numerical type: " + type);
        };
    }

    public <T> void encodeCatVariables(List<DataModel<T>> dataModels, Set<String> categoricalAttributes) {
        Map<String, Set<String>> uniqueCategoriesMap = new HashMap<>();

        for (DataModel<T> dataModel : dataModels) {
            for (DataAttributes<T> attribute : dataModel.getAttributesMap().values()) {
                if (categoricalAttributes.contains(attribute.getAttributeName())) {
                    uniqueCategoriesMap.computeIfAbsent(attribute.getAttributeName(), k -> new HashSet<>())
                            .add((String) attribute.getValue());
                }
            }
        }

        for (DataModel<T> dataModel : dataModels) {
            Map<String, DataAttributes<T>> updatedAttributes = new HashMap<>();

            for (Map.Entry<String, DataAttributes<T>> entry : dataModel.getAttributesMap().entrySet()) {
                String attributeName = entry.getKey();
                DataAttributes<T> attribute = entry.getValue();

                if (categoricalAttributes.contains(attributeName)) {
                    Set<String> uniqueCategories = uniqueCategoriesMap.get(attributeName);
                    encodeCategoricalData(attributeName, attribute, uniqueCategories, updatedAttributes);
                } else {
                    updatedAttributes.put(attributeName, attribute);
                }
            }

            dataModel.setAttributesMap(updatedAttributes);
        }
    }

    <T> void encodeCategoricalData(String attributeName, DataAttributes<T> attribute,
                                   Set<String> uniqueCategories,
                                   Map<String, DataAttributes<T>> updatedAttributes) {
        if (attribute.getValue() instanceof String categoryValue) {
            for (String uniqueCategory : uniqueCategories) {
                DataAttributes<T> newAttribute = new DataAttributes<>();
                newAttribute.setAttributeName(uniqueCategory);
                newAttribute.setType("java.lang.Integer");
                newAttribute.setValue(uniqueCategory
                        .equals(categoryValue) ? (T) Integer.valueOf(1)
                        : (T) Integer.valueOf(0));
                updatedAttributes.put(newAttribute.getAttributeName(), newAttribute);
            }
        } else {
            updatedAttributes.put(attributeName, attribute);
        }
    }


    public <T extends Number> void removeOutliers(final List<DataAttributes<T>> column) {
        final int firstQuartileFactor = 4;
        final int thirdQuartileFactor = 3;
        final double outlierThresholdFactor = 1.5;

        List<Double> values = column.stream()
                .map(attr -> attr.getValue().doubleValue())
                .sorted()
                .collect(Collectors.toList());

        double q1 = values.get(values.size() / firstQuartileFactor);
        double q3 = values.get(values.size() / firstQuartileFactor * thirdQuartileFactor);
        double iqr = q3 - q1;

        double lowerThreshold = q1 - outlierThresholdFactor * iqr;
        double upperThreshold = q3 + outlierThresholdFactor * iqr;

        column.removeIf(attr -> {
            double value = attr.getValue().doubleValue();
            return value < lowerThreshold || value > upperThreshold;
        });
    }
}
