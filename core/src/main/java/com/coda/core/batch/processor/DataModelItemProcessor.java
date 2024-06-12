package com.coda.core.batch.processor;

import com.coda.core.entities.DataModel;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.Constants;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
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

    @Autowired
    private DataModelProcessor dataModelProcessor;

    /**
     * The DataModelRepository object.
     */

    @Autowired
    private DataModelRepository dataModelRepository;

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
