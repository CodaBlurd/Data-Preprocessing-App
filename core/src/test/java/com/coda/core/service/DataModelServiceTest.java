package com.coda.core.service;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataExtractionException;
import com.coda.core.exceptions.DataLoadingException;
import com.coda.core.repository.DataModelRepository;
import com.coda.core.util.db.DatabaseExtractor;
import com.coda.core.util.db.DatabaseExtractorFactory;
import com.coda.core.util.file.FileExtractor;
import com.coda.core.util.transform.DataTransformation;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import java.io.ByteArrayInputStream;
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

    @Mock
    private DataTransformation dataTransformation;


    @InjectMocks
    private DataModelService dataModelService;


    private Map<String, DataModel<Document>> dataModels;



    @BeforeEach
    public void setUp() {
        dataModels = new HashMap<>();
        DataModel<Document> dataModel = new DataModel<>();
        Map<String, DataAttributes<Document>> attributesMap = new HashMap<>();
        attributesMap.put("key1", new DataAttributes<Document>("Age",
                new Document("key1", "value1"),
                "Document", Document.class));
        dataModel.setAttributesMap(attributesMap);
        dataModels.put("model1", dataModel);
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

        when(databaseExtractor.readData(tableName)).thenReturn(expectedDataModels);

        // Act
        List<DataModel<Object>> actualDataModels = dataModelService.extractDataFromTable(type, tableName);

        // Assert
        assertEquals(expectedDataModels, actualDataModels);
        verify(databaseExtractorFactory).getExtractor(type);
        verify(databaseExtractor).readData(tableName);
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
        assertThrows(RuntimeException.class, () ->
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

        assertThrows(DataLoadingException.class, () -> dataModelService.loadDataToCSV(dataModels, filePath));

        verify(fileExtractor).canWrite(filePath);
        verify(fileExtractor, never()).writeDataWithApacheCSV(any(), any());
    }

    @Test
    public void testLoadDataToSQL() throws Exception {
        // Mock dependencies
        when(databaseExtractorFactory.getExtractor(anyString())).thenReturn(databaseExtractor);

        // test DataModel
        Map<String, DataAttributes<Object>> attributes = new HashMap<>();
        attributes.put("column1", new DataAttributes<>("column1", "value1",
                "VARCHAR", Object.class));
        DataModel<Object> dataModel = new DataModel<>(new ObjectId("60c72b2f5f1b2c6f1f4b25a4"), attributes);
        List<DataModel<Object>> dataModels = Collections.singletonList(dataModel);

        // Execute the method
        dataModelService.loadDataToSQL(dataModels, "test_table", "MySQL");

        // Verify interactions
        verify(databaseExtractor).loadData(dataModels, "test_table");
        verify(databaseExtractorFactory).getExtractor("mysql");
    }

    @Test
    public void testLoadDataToMongoSuccess() throws Exception {
        String dbName = "testDb";
        String tableName = "testTable";
        String url = "mongodb://localhost:27017";
        String type = "mongo";

        when(databaseExtractorFactory.getExtractor(type.trim().toLowerCase())).thenReturn(databaseExtractor);

        assertDoesNotThrow(() -> {
            dataModelService.loadDataToMongo(dataModels, dbName, tableName, url, type);
        });

        verify(databaseExtractorFactory, times(1)).getExtractor(type.trim().toLowerCase());
        verify(databaseExtractor, times(1)).loadData(dataModels, dbName, tableName, url);
        verify(dataModelRepository, times(1)).saveAll(dataModels.values());
    }

    @Test
    public void testLoadDataToMongoNoExtractor() throws Exception {
        String dbName = "testDb";
        String tableName = "testTable";
        String url = "mongodb://localhost:27017";
        String type = "unknown";

        when(databaseExtractorFactory.getExtractor(type.trim().toLowerCase())).thenReturn(null);

        Exception exception = assertThrows(DataLoadingException.class, () -> {
            dataModelService.loadDataToMongo(dataModels, dbName, tableName, url, type);
        });

        assertEquals("Error loading data to MongoDB: "
                + "No suitable extractor for provided db type found", exception.getMessage());

        verify(databaseExtractorFactory, times(1)).getExtractor(type.trim().toLowerCase());
        verify(databaseExtractor, times(0)).loadData(any(), any(), any(), any());
        verify(dataModelRepository, times(0)).saveAll(any());
    }

    @Test
    public void testLoadDataToMongoException() throws Exception {
        String dbName = "testDb";
        String tableName = "testTable";
        String url = "mongodb://localhost:27017";
        String type = "mongo";

        when(databaseExtractorFactory.getExtractor(type.trim().toLowerCase())).thenReturn(databaseExtractor);
        doThrow(new RuntimeException("Database error")).when(databaseExtractor).loadData(any(), any(), any(), any());

        Exception exception = assertThrows(DataLoadingException.class, () -> {
            dataModelService.loadDataToMongo(dataModels, dbName, tableName, url, type);
        });

        assertTrue(exception.getMessage().contains("Error loading data to MongoDB"));

        verify(databaseExtractorFactory, times(1)).getExtractor(type.trim().toLowerCase());
        verify(databaseExtractor, times(1)).loadData(dataModels, dbName, tableName, url);
        verify(dataModelRepository, times(0)).saveAll(any());
    }






}

