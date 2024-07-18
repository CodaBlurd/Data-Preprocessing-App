package com.coda.core.util.db;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.DataLoadingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class MySQLExtractorTest {

    @Mock
    private ConnectionFactory connectionFactory;

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private Statement statement;

    @Mock
    private ResultSet resultSet;

    private List<DataModel<Object>> dataModels;

    @InjectMocks
    private MySQLExtractor extractor;

    @BeforeEach
    public void setUp() throws SQLException {
        MockitoAnnotations.openMocks(this);
        extractor.setConnectionFactory(connectionFactory);

        when(connectionFactory.dataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, false);
        when(resultSet.getObject(1)).thenReturn("value1");
        when(resultSet.getString("id")).thenReturn("60c72b2f5f1b2c6f1f4b25a4");
        when(resultSet.getMetaData()).thenReturn(mock(ResultSetMetaData.class));

        dataModels = new ArrayList<>();

        DataModel<Object> dataModel1 = new DataModel<>();
        Map<String, DataAttributes<Object>> attributes1 = new HashMap<>();
        attributes1.put("id", new DataAttributes<>("objectId",
                "60c72b2f5f1b2c6f1f4b25a4", "VARCHAR", Object.class));
        attributes1.put("name", new DataAttributes<>("products",
                "ipad", "VARCHAR", Object.class));
        dataModel1.setAttributesMap(attributes1);

        DataModel<Object> dataModel2 = new DataModel<>();
        Map<String, DataAttributes<Object>> attributes2 = new HashMap<>();
        attributes2.put("id", new DataAttributes<>("objectId", 2, "java.lang.Integer", Object.class));
        attributes2.put("name", new DataAttributes<>("Test Product 2", "Test Product 2", "VARCHAR", Object.class));
        dataModel2.setAttributesMap(attributes2);

        dataModels.add(dataModel1);
        dataModels.add(dataModel2);
    }

    @Test
    public void testReadData() throws Exception {
        // Mocking dependencies
        DataSource mockDataSource = mock(DataSource.class);
        Connection mockConnection = mock(Connection.class);
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
    public void testLoadData_success() throws SQLException {
        String tableName = "products_new";
        ResultSet mockResultSet = mock(ResultSet.class);

        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true, false);

        assertDoesNotThrow(() -> extractor.loadData(dataModels, tableName));

        verify(connectionFactory.dataSource(), times(25)).getConnection();
        verify(connection, times(14)).prepareStatement(anyString());
        verify(preparedStatement, times(2)).addBatch();
        verify(preparedStatement, times(1)).executeBatch();
        verify(connection, times(25)).close();
        verify(preparedStatement, times(14)).close();
    }

    @Test
    public void testLoadData_throwsIllegalArgumentExceptionWhenDataModelsAreNull() {
        List<DataModel<Object>> nullDataModels = null;
        String tableName = "products_new";

        IllegalArgumentException exception
                = assertThrows(IllegalArgumentException.class,
                () -> extractor.loadData(nullDataModels, tableName));
        assertEquals("DataModels cannot be null or empty", exception.getMessage());
    }

    @Test
    public void testLoadData_throwsIllegalArgumentExceptionWhenDataModelsAreEmpty() {
        List<DataModel<Object>> emptyDataModels = new ArrayList<>();
        String tableName = "products_new";

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> extractor.loadData(emptyDataModels, tableName));
        assertEquals("DataModels cannot be null or empty", exception.getMessage());
    }

//    @Test
//    public void testLoadData_throwsIllegalArgumentExceptionWhenDataModelIdIsNull() {
//        dataModels.get(0).getAttributesMap().remove("id");
//        String tableName = "products_new";
//
//        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
//                () -> extractor.loadData(dataModels, tableName));
//        assertEquals("Cannot read the array length because \"updateCounts\" is null", exception.getMessage());
//    }

    @Test
    public void testLoadData_handlesSQLException() throws SQLException {
        String tableName = "products_new";
        ResultSet mockResultSet = mock(ResultSet.class);

        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockResultSet.next()).thenReturn(true, false);
        doThrow(SQLException.class).when(connection).prepareStatement(anyString());

        DataLoadingException exception = assertThrows(DataLoadingException.class,
                () -> extractor.loadData(dataModels, tableName));
        assertTrue(exception.getMessage().contains("Error while loading data into database: "));
    }
}
