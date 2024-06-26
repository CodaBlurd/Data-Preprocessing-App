package com.coda.core.batch.processor;

import com.coda.core.entities.DataModel;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.Constants;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * The DataModelItemProcessor class.
 */

@Component
public class DataModelItemProcessor
        implements ItemProcessor<DataModel<Object>, DataModel<Object>> {

    /**
     * The DataModelProcessor object.
     */
    private final DataModelProcessor dataModelProcessor;

    /**
     * The DataModelRepository object.
     */

    private final DataModelRepository dataModelRepository;

    /**
     * Constructor for DataModelItemProcessor.
     * @param processor the DataModelProcessor object.
     * @param dataModelRepository the DataModelRepository object.
     */

    public DataModelItemProcessor(final DataModelProcessor processor,
            final DataModelRepository dataModelRepository) {
        this.dataModelProcessor = processor;
        this.dataModelRepository = dataModelRepository;
    }

    /**
     * Process the DataModel object.
     * @param item the DataModel object.
     * @return the DataModel object.
     * @throws Exception the Exception object.
     */

    @Override
    public DataModel<Object> process(@NonNull final DataModel<Object> item)
            throws Exception {
        dataModelProcessor.processAndSaveDataModels(List.of(item),
                Constants.BATCH_SIZE, dataModelRepository);
        return item;
    }
}
