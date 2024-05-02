package com.coda.core.service;

import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataExtractionException;
import com.coda.core.exceptions.ReadFromDbExceptions;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.db.DatabaseExtractor;
import com.coda.core.util.db.DatabaseExtractorFactory;
import com.coda.core.util.file.FileExtractor;
import com.coda.core.util.file.FileExtractorImpl;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.verification.VerificationMode;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
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

    @Mock
    FileExtractor fileExtractor;

    @Mock
    ResourceLoader resourceLoader;

    @InjectMocks
    private DataModelService dataModelService;



    @BeforeEach
    void setUp() {
    }
    /**
     * This method is used to test the extractDataFromTable method
     * @throws Exception if an error occurs
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
    }

    @Test
    void testExtractDataFromTableNoSQL() throws Exception {
        String type = "mongodb";
        String databaseName = "sampleDB";
        String tableName = "sampleTable";
        String url = "mongodb://localhost:27017";

        Map<String, DataModel<Document>> expectedDataModels = new HashMap<>();
        DataModel<Document> model = new DataModel<>();
        expectedDataModels.put("1", model);
        when(databaseExtractorFactory.getExtractor(anyString())).thenReturn(databaseExtractor);
        when(databaseExtractor.readData(databaseName, tableName, url)).thenReturn(expectedDataModels);

        Map<String, DataModel<Document>> result = dataModelService.extractDataFromTable(type, databaseName, tableName, url);

        assertNotNull(result);
        assertEquals(expectedDataModels, result);
        verify(dataModelRepository).saveAll(anyCollection());
    }

    @Test
    void extractDataFromTable_InvalidArguments() {
        // Arrange
        String type = null;
        String databaseName = "testDB";
        String tableName = "testTable";
        String url = "mongodb://localhost:27017";

        // Act & Assert
        assertThrows(IllegalArgumentException.class, () ->
                dataModelService.extractDataFromTable(type, databaseName, tableName, url));
    }


    @Test
    void extractDataFromTable_ReadFromDbExceptions() throws Exception {
        // Arrange
        String type = "mongodb";
        String databaseName = "testDB";
        String tableName = "testTable";
        String url = "mongodb://localhost:27017";

        when(databaseExtractorFactory.getExtractor(type)).thenReturn(databaseExtractor);
        when(databaseExtractor.readData(databaseName, tableName, url)).thenThrow(new RuntimeException("Database error"));

        // Act & Assert
        assertThrows(ReadFromDbExceptions.class, () ->
                dataModelService.extractDataFromTable(type, databaseName, tableName, url));
    }

    @Test
    public void testExtractDataFromFile_Valid() throws Exception {
        String resourcePath = "classpath:valid.csv";
        List<DataModel<Object>> expectedDataModels = new ArrayList<>();
        Resource mockResource = mock(Resource.class);
        InputStream mockInputStream = new ByteArrayInputStream(new byte[0]);

        // Mocking the resource loader and file system behaviors
        when(resourceLoader.getResource(resourcePath)).thenReturn(mockResource);
        when(mockResource.exists()).thenReturn(true);
        when(mockResource.getInputStream()).thenReturn(mockInputStream);

        // Ensuring that the file extractor reads the mock input stream and returns the expected data models
        when(fileExtractor.readDataWithApacheCSV(mockInputStream)).thenReturn(expectedDataModels);

        // Execute the method under test
        List<DataModel<Object>> actualDataModels = dataModelService.extractDataFromFile("valid.csv");

        // Assertions and verifications
        assertNotNull(actualDataModels, "The result should not be null");
        assertEquals(expectedDataModels, actualDataModels, "The returned data models should match the expected");
        verify(mockResource).getInputStream();
        verify(fileExtractor).readDataWithApacheCSV(mockInputStream);
    }


    @Test
    public void testExtractDataFromFile_FileNotFound() {
        String resourcePath = "classpath:invalid.csv";
        Resource mockResource = mock(Resource.class);

        // Mocking the resource loader and file system behaviors
        when(resourceLoader.getResource(resourcePath)).thenReturn(mockResource);
        when(mockResource.exists()).thenReturn(false);

        // Execute the method under test
        assertThrows(DataExtractionException.class, () -> dataModelService.extractDataFromFile("invalid.csv"));

        // Verifying that the resource loader was called to get the resource
        verify(resourceLoader).getResource(resourcePath);
    }

    @Test
    void testLoadDataToCSV() throws IOException {
        // arrange
        String filePath = "test.csv";
        List<DataModel<Object>> dataModels = new ArrayList<>();
        dataModels.add(new DataModel<>());
        dataModels.add(new DataModel<>());
        when(fileExtractor.canWrite(filePath)).thenReturn(true);
        doNothing().when(fileExtractor).writeDataWithApacheCSV(dataModels, filePath);

        // act
        dataModelService.loadDataToCSV(dataModels, filePath);

        // assert
        verify(fileExtractor).canWrite(filePath);
        verify(fileExtractor).writeDataWithApacheCSV(dataModels, filePath);

    }

    @Test
    void testLoadDataToCSV_FailToWrite() throws IOException {
        String filePath = "test.csv";
        List<DataModel<Object>> dataModels = new ArrayList<>();
        when(fileExtractor.canWrite(filePath)).thenReturn(false);

        assertThrows(DataExtractionException.class, () -> dataModelService.loadDataToCSV(dataModels, filePath));

        verify(fileExtractor).canWrite(filePath);
        verify(fileExtractor, never()).writeDataWithApacheCSV(any(), any());
    }






}

