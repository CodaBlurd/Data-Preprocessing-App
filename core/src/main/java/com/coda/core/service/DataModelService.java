package com.coda.core.service;

import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;

/**
 * Service class for DataModel entity
 * <p> This class is responsible for all the business logic related to DataModel entity</p>
 * {@code @Service} annotation to mark the class as a service class
 * {@code @Slf4j} annotation to enable logging
 * {@code @Autowired} annotation to inject the DataModelRepository
 */

@Service
@Slf4j
public class DataModelService {

    private final DataModelRepository dataModelRepository;
    private final DatabaseExtractorFactory databaseExtractorFactory;



    @Autowired
    public DataModelService(DataModelRepository dataModelRepository,
                            DatabaseExtractorFactory databaseExtractorFactory) {
        this.dataModelRepository = dataModelRepository;
        this.databaseExtractorFactory = databaseExtractorFactory;
    }

    //== public methods ==

    //== DATA EXTRACTION PHASE ==

    /**
     * Reads data from a database.
     * @param tableName The name of the table to read from
     * @param type The type of the database
     *  The table name must be a valid table name
     * @param url The url of the database
     * @param user The username of the database
     * @param password The password of the database
     * @return A list of DataModel objects
     * @throws ReadFromDbExceptions if the table name is invalid
     */

    @Transactional
    public List<DataModel<Object>> extractDataFromTable(String type, String tableName, String url, String user, String password)
            throws ReadFromDbExceptions {
        if (type == null || tableName == null || url == null || user == null || password == null) {
            throw new IllegalArgumentException("Invalid arguments");
        }

        // extract data from the database
        try{
            // check the database type
            DatabaseExtractor databaseExtractor =   databaseExtractorFactory.getExtractor(type);
            List<DataModel<Object>> dataModels = databaseExtractor.readData(tableName, url, user, password);
            dataModelRepository.saveAll(dataModels);
            return dataModels;
        } catch (Exception e) {
            log.error("Error while reading data from database", e);
            throw new ReadFromDbExceptions("Error reading from database",
                    ErrorType.READ_FROM_DB_EXCEPTIONS);
        }





    }









    //== DATA TRANSFORMATION PHASE ==




    //== DATA LOADING PHASE ==


}
