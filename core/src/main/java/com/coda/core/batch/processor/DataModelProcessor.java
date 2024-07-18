package com.coda.core.batch.processor;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataExtractionException;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.transform.DataTransformation;
import com.coda.core.util.types.ErrorType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Component
@Slf4j
public class DataModelProcessor {

    private final DataTransformation dataTransformation;

    public DataModelProcessor(DataTransformation transformation) {
        this.dataTransformation = transformation;
    }

    public void processAndSaveDataModels(final List<DataModel<Object>> dataModels,
                                         final int batchSize,
                                         final DataModelRepository dataModelRepository)
            throws DataExtractionException, ClassNotFoundException {
        validateDataModels(dataModels);

        for (DataModel<Object> dataModel : dataModels) {
            if (dataModel.getAttributesMap() != null) {
                for (DataAttributes<?> dataAttributes : dataModel.getAttributesMap().values()) {
                    processDataAttributes(dataAttributes);
                }
            }
        }

        // Normalize the data set
        normalizeDataSet(dataModels);

        List<List<DataModel<Object>>> partitions = partitionList(dataModels, batchSize);
        for (List<DataModel<Object>> batch : partitions) {
            saveProcessedDataModels(dataModelRepository, batch);
        }
    }

    public void saveProcessedDataModels(final DataModelRepository dataModelRepository,
                                        final List<DataModel<Object>> dataModels)
            throws DataExtractionException {
        try {
            dataModelRepository.saveAll(dataModels);
        } catch (Exception e) {
            log.error("Error saving data models to repository", e);
            throw new DataExtractionException("Error saving data models: "
                    + e.getMessage(), ErrorType.DATA_SAVE_ERROR);
        }
    }

    private <T> void normalizeDataSet(List<DataModel<T>> dataModels) {
        Objects.requireNonNull(dataModels, "Data models cannot be null");
        try {
            dataTransformation.normalize(dataModels);

            Set<String> categoricalAttributes = new HashSet<>();
            dataModels.forEach(dataModel -> dataModel.getAttributesMap().values()
                    .forEach(dataAttributes -> {
                if (dataAttributes.getType().equals("java.lang.String")) {
                    categoricalAttributes.add(dataAttributes.getAttributeName());
                }
            }));

            dataTransformation.encodeCatVariables(dataModels, categoricalAttributes);
        } catch (ClassNotFoundException e) {
            log.error("Error normalizing data models: {}", e.getMessage());
        }
    }

    private <T> void processDataAttributes(DataAttributes<T> dataAttributes)
            throws DataExtractionException, ClassNotFoundException {
        String type = Objects.requireNonNullElse(dataAttributes.getType(), "");
        T value = dataAttributes.getValue();

        if (isValueNullOrEmpty(value)) {
            dataAttributes.applyDefaultValue();
            value = dataAttributes.getValue();
        }

        String format = Objects.requireNonNullElse(dataAttributes.getFormat(), "");
        String attributeName = Objects.requireNonNullElse(dataAttributes.getAttributeName(), "");

        value = transformAndCleanValue(type, value, format, attributeName);
        dataAttributes.setValue(value);

        processAttributeByType(type, dataAttributes);

        dataAttributes.applyDefaultValue();
        validateAttribute(dataAttributes);
        dataAttributes.setLastUpdatedDate(Instant.now());
    }

    private <T> boolean isValueNullOrEmpty(T value) {
        return value == null || (value instanceof String && ((String) value).isEmpty());
    }

    private <T> void processAttributeByType(String type, DataAttributes<T> dataAttributes)
            throws DataExtractionException, ClassNotFoundException {
        if (isNumericType(type)) {
            processNumericAttribute(List.of(dataAttributes));
        } else if (isStringType(type)) {
            processStringAttribute(dataAttributes);
        } else if (isDateType(type)) {
            processDateAttribute(dataAttributes);
        } else if (isBooleanType(type)) {
            processBooleanAttribute(dataAttributes);
        } else if ("java.lang.Object".equals(type)) {
            processObjectAttribute(dataAttributes);
        } else {
            throw new DataExtractionException("Unknown attribute type: "
                    + type, ErrorType.UNKNOWN_ATTRIBUTE_TYPE);
        }
    }

    private <T> void processBooleanAttribute(DataAttributes<T> attributes) {
        List<DataAttributes<Boolean>> booleanAttributes = convertToBooleanAttributes(List.of(attributes));
        if (!booleanAttributes.isEmpty()) {
            DataAttributes<Boolean> booleanDataAttributes = booleanAttributes.get(0);
            dataTransformation.transformValue(booleanDataAttributes.getType(),
                    booleanDataAttributes.getValue(), booleanDataAttributes.getFormat(),
                    booleanDataAttributes.getAttributeName());
        }
    }

    private <T> List<DataAttributes<Boolean>> convertToBooleanAttributes(final List<DataAttributes<T>> attributes) {
        return attributes.stream()
                .filter(attr -> attr.getValue() instanceof Boolean)
                .map(attr -> new DataAttributes<>(attr.getAttributeName(),
                        attr.getValue(), attr.getType(),
                        Boolean.class))
                .collect(Collectors.toList());
    }

    private boolean isBooleanType(String type) {
        return "java.lang.Boolean".equals(type) || "BOOLEAN".equals(type);
    }

    private boolean isDateType(String type) {
        return "java.time.LocalDateTime".equals(type)
                || "java.time.LocalDate".equals(type)
                || "java.time.LocalTime".equals(type)
                || "java.util.Date".equals(type)
                || "java.sql.Date".equals(type)
                || "java.sql.Timestamp".equals(type);
    }

    private <T> void processDateAttribute(DataAttributes<T> dataAttributes) {
        List<DataAttributes<LocalDateTime>> dateAttributes = convertToDateAttributes(List.of(dataAttributes));
        if (!dateAttributes.isEmpty()) {
            DataAttributes<LocalDateTime> dateAttribute = dateAttributes.get(0);
            dataTransformation.transformValue(dateAttribute.getType(),
                    dateAttribute.getValue(),
                    dateAttribute.getFormat(),
                    dateAttribute.getAttributeName());
        }
    }

    private <T> List<DataAttributes<LocalDateTime>> convertToDateAttributes(final List<DataAttributes<T>> attributes) {
        return attributes.stream()
                .filter(attr -> attr.getValue() instanceof LocalDateTime)
                .map(attr -> new DataAttributes<>(attr.getAttributeName(),
                        attr.getValue(), attr.getType(),
                        LocalDateTime.class))
                .collect(Collectors.toList());
    }

    private <T> T transformAndCleanValue(String type, T value, String format, String attributeName)
            throws ClassNotFoundException {
        value = dataTransformation.transformValue(type, value, format, attributeName);
        value = dataTransformation.cleanCategoricalValues(type, value);
        return value;
    }

    private <T> void processNumericAttribute(List<DataAttributes<T>> dataAttributes)
            throws ClassNotFoundException {
        List<DataAttributes<Number>> numberAttributes = convertToNumberAttributes(dataAttributes);
        if (!numberAttributes.isEmpty()) {
            DataAttributes<Number> numberAttribute = numberAttributes.get(0);
            dataTransformation.removeOutliers(numberAttributes);
            dataTransformation.replaceMissingNumericalValues(numberAttributes, numberAttribute);
        }
    }

    private <T> void processStringAttribute(DataAttributes<T> dataAttributes) {
        List<DataAttributes<T>> attributes = List.of(dataAttributes);
        dataTransformation.replaceMissingCategoricalValues(attributes, dataAttributes.getType());
    }

    private boolean isNumericType(String type) {
        return "java.lang.Integer".equals(type)
                || "java.lang.Double".equals(type)
                || "java.lang.Long".equals(type)
                || "java.lang.Float".equals(type)
                || "java.math.BigDecimal".equals(type);
    }

    private boolean isStringType(String type) {
        return "java.lang.String".equals(type)
                || "TEXT".equals(type)
                || "VARCHAR".equals(type);
    }

    private <T> List<DataAttributes<Number>> convertToNumberAttributes(List<DataAttributes<T>> attributes) throws ClassNotFoundException {
        List<DataAttributes<Number>> numberAttributes = new ArrayList<>();
        for (DataAttributes<T> attribute : attributes) {
            String typeClazzName = attribute.getTypeClazzName();
            Class<?> clazz = Class.forName(typeClazzName);

            if (Number.class.isAssignableFrom(clazz) && !clazz.equals(Number.class)) {
                numberAttributes.add((DataAttributes<Number>) attribute);
            } else {
                log.warn("Unsupported numerical type: {}", typeClazzName);
            }
        }
        return numberAttributes;
    }

    private void validateDataModels(final List<DataModel<Object>> dataModels)
            throws DataExtractionException {
        if (dataModels == null || dataModels.isEmpty()) {
            throw new DataExtractionException("Data models list cannot be null or empty.",
                    ErrorType.DATA_EXTRACTION_FAILED);
        }
    }

    private <T> void validateAttribute(final DataAttributes<T> dataAttributes)
            throws DataExtractionException {
        if (!dataAttributes.applyValidationRules()) {
            throw new DataExtractionException("Validation failed for attribute: "
                    + dataAttributes.getAttributeName(),
                    ErrorType.VALIDATION_FAILED);
        }
    }

    private <T> List<List<T>> partitionList(final List<T> list, final int size) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }

    private <T> void processObjectAttribute(DataAttributes<T> dataAttributes)
            throws ClassNotFoundException, DataExtractionException {
        Object value = dataAttributes.getValue();
        if (value instanceof Boolean) {
            processBooleanAttribute((DataAttributes<Boolean>) dataAttributes);
        } else if (value instanceof Number) {
            processNumericAttribute(List.of(dataAttributes));
        } else if (value instanceof String) {
            processStringAttribute((DataAttributes<String>) dataAttributes);
        } else if (value instanceof LocalDateTime) {
            processDateAttribute((DataAttributes<LocalDateTime>) dataAttributes);
        } else {
            throw new DataExtractionException("Unsupported object type: "
                    + value.getClass(),
                    ErrorType.UNKNOWN_ATTRIBUTE_TYPE);
        }
    }
}
