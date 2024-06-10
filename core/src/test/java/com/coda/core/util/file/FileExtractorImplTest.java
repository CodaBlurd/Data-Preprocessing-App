package com.coda.core.util.file;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class FileExtractorImplTest {

    @Mock
    private FileExtractor extractor;

    @InjectMocks
    private FileExtractorImpl fileService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testReadDataWithApacheCSV_FilePath() throws Exception {
        // Mock the input stream
        String csvContent = "column1,column2\nvalue1,value2\n";
        InputStream inputStream = new ByteArrayInputStream(csvContent.getBytes());

        // Mock the file input stream
        Path path = Paths.get("test.csv");
        Files.write(path, csvContent.getBytes());

        // Execute the method
        List<DataModel<Object>> dataModels = fileService.readDataWithApacheCSV("test.csv");

        // Verify the results
        assertNotNull(dataModels);
        assertEquals(1, dataModels.size());
        assertEquals("value1", dataModels.get(0).getAttributesMap().get("column1").getValue());

        // Clean up
        Files.delete(path);
    }

    @Test
    public void testReadDataWithApacheCSV_InputStream() throws Exception {
        // Mock the input stream
        String csvContent = "column1,column2\nvalue1,value2\n";
        InputStream inputStream = new ByteArrayInputStream(csvContent.getBytes());

        // Execute the method
        List<DataModel<Object>> dataModels = fileService.readDataWithApacheCSV(inputStream);

        // Verify the results
        assertNotNull(dataModels);
        assertEquals(1, dataModels.size());
        assertEquals("value1", dataModels.get(0).getAttributesMap().get("column1").getValue());
    }

    @Test
    public void testWriteDataWithApacheCSV() throws Exception {
        // Create a test DataModel
        Map<String, DataAttributes<Object>> attributes = new HashMap<>();
        attributes.put("column1", new DataAttributes<>("column1", "value1", "VARCHAR", Object.class));
        DataModel<Object> dataModel = new DataModel<>(new ObjectId("60c72b2f5f1b2c6f1f4b25a4"), attributes);
        List<DataModel<Object>> dataModels = Collections.singletonList(dataModel);

        // Mock the file path
        Path path = Paths.get("test_output.csv");

        // Execute the method
        fileService.writeDataWithApacheCSV(dataModels, "test_output.csv");

        // Verify the file content
        List<String> lines = Files.readAllLines(path);
        assertEquals("column1", lines.get(0)); // Header
        assertEquals("value1", lines.get(1)); // Data

        // Clean up
        Files.delete(path);
    }

    @Test
    public void testExists() {
        // Mock the file path
        Path path = Paths.get("test_exists.csv");
        assertFalse(fileService.exists("")); // Empty path
        assertFalse(fileService.exists(null)); // Null path

        try {
            Files.createFile(path);
            assertTrue(fileService.exists("test_exists.csv")); // File exists
        } catch (IOException e) {
            fail("Failed to create test file");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testCanRead() {
        // Mock the file path
        Path path = Paths.get("test_can_read.csv");
        assertFalse(fileService.canRead("")); // Empty path
        assertFalse(fileService.canRead(null)); // Null path

        try {
            Files.createFile(path);
            assertTrue(fileService.canRead("test_can_read.csv")); // File is readable
        } catch (IOException e) {
            fail("Failed to create test file");
        } finally {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testCanWrite() {
        // Mock the file path
        Path path = Paths.get("test_can_write.csv");
        assertFalse(fileService.canWrite("")); // Empty path
        assertFalse(fileService.canWrite(null)); // Null path

        try {
            Files.createFile(path);
            assertTrue(fileService.canWrite("test_can_write.csv")); // File is writable
        } catch (IOException e) {
            fail("Failed to create test file");
        } finally {
            try {
                Files.delete(path);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
