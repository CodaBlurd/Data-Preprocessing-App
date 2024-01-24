package com.coda.core.service;

import com.coda.core.entities.DataModel;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.DatabaseExtractor;
import com.coda.core.util.DatabaseExtractorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Test class for DataModelService
 * <p> This class is responsible for testing the DataModelService class</p>
 * {@code @ExtendWith} annotation to enable MockitoExtension for Mockito to work with JUnit 5
 * {@code @Mock} annotation to mock the DataModelRepository and DatabaseExtractorFactory
 * {@code @InjectMocks} annotation to inject the DataModelService
 */

@ExtendWith(MockitoExtension.class)
public class DataModelServiceTest {

    @Mock
    private DataModelRepository dataModelRepository;

    @Mock
    private DatabaseExtractorFactory databaseExtractorFactory;

    @Mock
    private DatabaseExtractor databaseExtractor;

    @InjectMocks
    private DataModelService dataModelService;

    /**
     * This method is used to test the extractDataFromTable method
     * @throws Exception
     */

    @Test
    public void testExtractDataFromTable() throws Exception {
        // Arrange
        String type = "mysql";
        String tableName = "testTable";
        String url = "jdbc:mysql://localhost:3306/mydb";
        String user = "user";
        String password = "password";

        when(databaseExtractorFactory.getExtractor(type)).thenReturn(databaseExtractor);

        List<DataModel<Object>> expectedDataModels = new ArrayList<>();
        when(databaseExtractor.readData(tableName, url, user, password)).thenReturn(expectedDataModels);

        // Act
        List<DataModel<Object>> actualDataModels = dataModelService.extractDataFromTable(type, tableName, url, user, password);

        // Assert
        assertEquals(expectedDataModels, actualDataModels);
        verify(databaseExtractorFactory).getExtractor(type);
        verify(databaseExtractor).readData(tableName, url, user, password);
        verify(dataModelRepository).saveAll(expectedDataModels);
    }
}

