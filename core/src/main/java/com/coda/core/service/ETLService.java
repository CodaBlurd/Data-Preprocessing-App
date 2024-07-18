package com.coda.core.service;

import com.coda.core.dtos.ConnectionDetails;
import com.coda.core.entities.DataModel;
import com.coda.core.util.types.ErrorType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
public class ETLService {

    private final DataModelService dataModelService;

    public ETLService(DataModelService dataModelService) {
        this.dataModelService = dataModelService;
    }

    /**
     * Perform ETL process.
     *
     * @param connectionDetails the connection details
     * @param sourceDbType the source db type
     * @param sourceTableName the source table name
     * @param targetTableName the target table name
     * @param targetDbType the target db type
     */

    @Transactional
    public void performETLProcess(ConnectionDetails connectionDetails,
                                  String sourceDbType, String sourceTableName,
                                  String targetTableName, String targetDbType) {
        try {
            // Extract data
            List<DataModel<Object>> extractedData
                    = dataModelService.extractDataFromTable(connectionDetails,
                    sourceDbType, sourceTableName);
            extractedData.forEach(dataModel -> log.info("Extracted data: {}", dataModel));

            // Transform data - handled within extraction method (processAndSaveDataModels)

            // Load data to target database
            dataModelService.loadDataToSQL(extractedData, targetTableName, targetDbType);

            log.info("ETL process completed successfully.");
        } catch (Exception e) {
            log.error("Error during ETL process", e);
            throw new ETLException("Error during ETL process",
                    ErrorType.ETL_PROCESS_FAILED);
        }
    }

    @Getter
    private static class ETLException extends RuntimeException {
        private final String message;
        private final ErrorType errorType;
        public ETLException(String message, ErrorType type) {
            this.message = message;
            this.errorType = type;
        }
    }
}

