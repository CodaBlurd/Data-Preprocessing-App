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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Slf4j
public class DataModelProcessor {

    /**
     * The DataTransformation Object.
     */
    private final DataTransformation dataTransformation;

    /**
     * Constructor for DataModelProcessor.
     * @param transformation the DataTransformation object.
     */
    public DataModelProcessor(final DataTransformation transformation) {
        this.dataTransformation = transformation;
    }

    /**
     * Process and save the data models.
     * @param dataModels the list of data models.
     * @param batchSize the batch size.
     * @param dataModelRepository the data model repository.
     * @throws DataExtractionException the data extraction exception.
     * @throws ClassNotFoundException the class not found exception.
     */

    public void processAndSaveDataModels(
            final List<DataModel<Object>> dataModels, final int batchSize,
            final DataModelRepository dataModelRepository)
            throws DataExtractionException, ClassNotFoundException {

        validateDataModels(dataModels);
        Map<String, List<DataAttributes<Object>>> categorizedAttributes
                = categorizeAttributesByType(dataModels);
        processAttributes(categorizedAttributes);
        List<List<DataModel<Object>>> partitions
                = partitionList(dataModels, batchSize);
        for (List<DataModel<Object>> batch : partitions) {
            saveProcessedDataModels(dataModelRepository, batch);
        }
    }

    /**
     * Saves the processed data models to the repository.
     * @param dataModelRepository the data model repository.
     * @param dataModels the list of data models.
     * @throws DataExtractionException the data extraction exception.
     */

    public void saveProcessedDataModels(
            final DataModelRepository dataModelRepository,
            final List<DataModel<Object>> dataModels)
            throws DataExtractionException {

        try {
            dataModelRepository.saveAll(dataModels);
        } catch (Exception e) {
            log.error("Error saving data models to repository", e);
            throw new DataExtractionException("Error saving "
                    + "data models: "
                    + e.getMessage(), ErrorType.DATA_SAVE_ERROR);
        }
    }

    //== Private Methods ==

    private void validateDataModels(final List<DataModel<Object>> dataModels)
            throws DataExtractionException {

        if (dataModels == null) {
            throw new DataExtractionException("Data models "
                    + "list cannot be null.",
                    ErrorType.DATA_EXTRACTION_FAILED);
        }
    }

    private Map<String, List<DataAttributes<Object>>>
    categorizeAttributesByType(
            final List<DataModel<Object>> dataModels) {

        return dataModels.stream()
                .flatMap(dataModel -> dataModel.getAttributesMap()
                        .values().stream())
                .collect(Collectors.groupingBy(DataAttributes::getType));
    }

    private void processAttributes(final Map<String,
            List<DataAttributes<Object>>> categorizedAttributes)
            throws DataExtractionException, ClassNotFoundException {

        for (Map.Entry<String, List<DataAttributes<Object>>> entry
                : categorizedAttributes.entrySet()) {
            String attributeType = entry.getKey();
            List<DataAttributes<Object>> attributes = entry.getValue();
            switch (attributeType) {
                case "numerical" -> processNumericAttributes(attributes);
                case "categorical" -> processCategoricalAttributes(attributes);
                default ->
                        throw new DataExtractionException("Unknown attribute"
                                + " type: " + attributeType,
                                ErrorType.UNKNOWN_ATTRIBUTE_TYPE);
            }
        }
    }

    private void processNumericAttributes(
            final List<DataAttributes<Object>> attributes)
            throws DataExtractionException, ClassNotFoundException {

        List<DataAttributes<Number>> numericAttributes
                = attributes.stream()
                .filter(attr -> attr.getValue() instanceof Number)
                .map(attr -> new DataAttributes<>(attr.getAttributeName(),
                        attr.getValue(), attr.getType(), Number.class))
                .collect(Collectors.toList());
        dataTransformation.removeOutliers(numericAttributes);

        for (DataAttributes<Object> dataAttributes : attributes) {
            dataTransformation.replaceMissingNumericalValues(
                    List.of(dataAttributes), dataAttributes);

            dataTransformation.normalizeData(List.of(dataAttributes),
                    dataAttributes);
            validateAttributes(dataAttributes);
        }
    }

    private void processCategoricalAttributes(
            final List<DataAttributes<Object>> attributes)
            throws DataExtractionException, ClassNotFoundException {

        for (DataAttributes<Object> dataAttributes : attributes) {
            String type = dataAttributes.getType();

            Object value = dataAttributes.getValue();
            String typeClazzName = dataAttributes.getTypeClazzName();

            dataTransformation.cleanCategoricalValues(type, value,
                    typeClazzName);
            dataTransformation
                    .replaceMissingCategoricalValues(
                            List.of(dataAttributes), type);

            dataTransformation
                    .encodeCategoricalData(
                            List.of(dataAttributes));

            validateAttributes(dataAttributes);
        }
    }

    private void validateAttributes(
            final DataAttributes<Object> dataAttributes)
            throws DataExtractionException {

        if (dataAttributes.applyValidationRules()) {
            throw new DataExtractionException("Validation failed "
                    + "for attribute: " + dataAttributes.getAttributeName(),
                    ErrorType.VALIDATION_FAILED);
        }
        dataAttributes.setLastUpdatedDate(Instant.now());
    }

    private <T> List<List<T>> partitionList(
            final List<T> list, final int size) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size,
                    list.size())));
        }
        return partitions;
    }
}
