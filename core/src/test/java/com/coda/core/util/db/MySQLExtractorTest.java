package com.coda.core.util.db;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class MySQLExtractorTest {

    @Mock
    private ConnectionFactory connectionFactory;

    @Mock
    private DataSource dataSource;



    @InjectMocks
    private MySQLExtractor extractor;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        extractor = new MySQLExtractor();
        extractor.setConnectionFactory(connectionFactory);
    }

    @Test
    public void testReadData() throws Exception {
        // Mocking dependencies
        Connection mockConnection = mock(Connection.class);
        DataSource mockDataSource = mock(DataSource.class);
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        ResultSet mockResultSet = mock(ResultSet.class);
        ResultSetMetaData mockResultSetMetaData = mock(ResultSetMetaData.class);

        String mockQuery = "SELECT * FROM test_table LIMIT ? OFFSET ?";

        // Setting up the mocks
        when(connectionFactory.dataSource()).thenReturn(mockDataSource);
        when(mockDataSource.getConnection()).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(mockQuery)).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
        when(mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(mockResultSetMetaData.getColumnName(1)).thenReturn("column1");
        when(mockResultSetMetaData.getColumnTypeName(1)).thenReturn("VARCHAR");
        when(mockResultSet.next()).thenReturn(true, false); // ResultSet has one row
        when(mockResultSet.getString("id")).thenReturn("60c72b2f5f1b2c6f1f4b25a4");
        when(mockResultSet.getObject(1)).thenReturn("value1");

        // Execute the method
        List<DataModel<Object>> dataModels = extractor.readData("test_table", 10, 0);

        // Verify results
        assertNotNull(dataModels);
        assertEquals(1, dataModels.size()); // Expect one DataModel in the list
        DataModel<Object> dataModel = dataModels.get(0);
        assertNotNull(dataModel.getAttributesMap().get("column1"));
        assertEquals("value1", dataModel.getAttributesMap().get("column1").getValue());

        // Verify the id is correctly parsed
        assertEquals("60c72b2f5f1b2c6f1f4b25a4", dataModel.getId().toHexString());

        // Verify that the prepared statement parameters were set correctly
        verify(mockPreparedStatement).setInt(1, 10);
        verify(mockPreparedStatement).setInt(2, 0);
    }

        @Test
    public void testLoadData() throws Exception {
        // Mocking dependencies
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        DataSource  mockDataSource = mock(DataSource.class);

        // Setting up the mocks
        when(connectionFactory.dataSource()).thenReturn(mockDataSource);
        when(connectionFactory.dataSource().getConnection()).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.executeBatch()).thenReturn(new int[]{1}); // Mock executeBatch to return non-null

        // Creating a test DataModel
        Map<String, DataAttributes<Object>> attributes = new HashMap<>();
        attributes.put("column1", new DataAttributes<>("column1",
                "value1", "VARCHAR", Object.class));
        DataModel<Object> dataModel = new DataModel<>(new ObjectId(
                "60c72b2f5f1b2c6f1f4b25a4"), attributes);
        List<DataModel<Object>> dataModels = Collections.singletonList(dataModel);

        // Execute the method
        extractor.loadData(dataModels, "test_table");

        // Verify that setObject was called with the correct parameters
        verify(mockPreparedStatement).setObject(1, "value1");

        // Verify that addBatch and executeBatch were called
        verify(mockPreparedStatement).addBatch();
        verify(mockPreparedStatement).executeBatch();
    }






}

