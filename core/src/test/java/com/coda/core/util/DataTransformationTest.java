package com.coda.core.util;

import com.coda.core.entities.DataAttributes;
import com.coda.core.exceptions.TransformationException;
import com.coda.core.util.transform.DataTransformation;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
public class DataTransformationTest {
    private DataTransformation dataTransformation;

    @BeforeEach
    public void setUp() {
        dataTransformation = new DataTransformation();
    }

    @Test
    public void testTransformValue_NullValue() {
        assertThrows(TransformationException.class, () -> {
            dataTransformation.transformValue("Integer",
                    null,
                    "java.lang.Integer", null,
                    "age");
        });
    }

    @Test
    public void testTransformValue_NoTransformationStrategy() {
        assertThrows(TransformationException.class, () -> {
            dataTransformation.transformValue("UnknownType", "123",
                    "java.lang.Integer", null, "age");
        });
    }

    @Test
    public void testTransformValue_Success() {
        Integer transformedValue = dataTransformation.transformValue("Integer",
                "123", "java.lang.Integer",
                null, "age");
        assertEquals(123, transformedValue);
    }

    @Test
    public void testCleanCategoricalValues_NonStringType() throws ClassNotFoundException {
        Object value = dataTransformation.cleanCategoricalValues("Integer",
                "123",
                "java.lang.Integer");
        assertEquals("123", value);
    }

    @Test
    public void testCleanCategoricalValues_NonStringValue() throws ClassNotFoundException {
        Object value = dataTransformation.cleanCategoricalValues("String", 123,
                "java.lang.String");
        assertEquals(123, value);
    }

    @Test
    public void testCleanCategoricalValues_StringValue() throws ClassNotFoundException {
        String value = dataTransformation.cleanCategoricalValues("String",
                "Hello, World!", "java.lang.String");
        assertEquals("HelloWorld", value);
    }

    @Test
    public void testReplaceMissingCategoricalValues_NonStringType() {
        assertThrows(TransformationException.class, () -> {
            List<DataAttributes<Object>> column = new ArrayList<>();
            // Add some data to the column.
            dataTransformation.replaceMissingCategoricalValues(column, "Integer");
        });
    }

    @Test
    public void testReplaceMissingCategoricalValues_StringType_MissingValues() {
        List<DataAttributes<String>> column = new ArrayList<>();
        // Add some data to the column, including null values.
        dataTransformation.replaceMissingCategoricalValues(column, "String");
        // Add assertions here to verify the behavior of the replaceMissingCategoricalValues method.
        assertEquals(0, column.size());
    }

    @Test
    public void testReplaceMissingCategoricalValues_StringType_NoMissingValues() {
        List<DataAttributes<String>> column = new ArrayList<>();
        // Add some data to the column, without null values.
        column.add(new DataAttributes<>("Name", "John",
                "String", String.class));
        column.add(new DataAttributes<>("Age", "30",
                "String", String.class));
        // Add some data to the column, with no null values.
        dataTransformation.replaceMissingCategoricalValues(column, "String");
        // Add assertions here to verify the behavior of the replaceMissingCategoricalValues method.
        assertEquals(2, column.size());
        assertEquals("John", column.get(0).getValue());
        assertEquals("30", column.get(1).getValue());
        assertEquals("String", column.get(0).getType());

    }

    @Test
    public void testReplaceMissingNumericalValues_NonNumberType() {
    assertThrows(ArithmeticException.class, () -> {
        List<DataAttributes<String>> column = new ArrayList<>();
        // Add some data to the column.
        DataAttributes<String> attribute = new DataAttributes<>("Name",
                "John", "String", String.class);
        dataTransformation.replaceMissingNumericalValues(column, attribute);
    });
}

@Test
public void testReplaceMissingNumericalValues_NumberType_MissingValues() throws ClassNotFoundException {
    List<DataAttributes<Integer>> column = new ArrayList<>();
    // Add some data to the column, including null values.
    column.add(new DataAttributes<>("Age", null, "Integer", Integer.class));
    column.add(new DataAttributes<>("Height", 175, "Integer", Integer.class));
    column.add(new DataAttributes<>("Weight", 70, "Integer", Integer.class));
    DataAttributes<Integer> attribute = new DataAttributes<>("Age", null,
            "Integer", Integer.class);
    dataTransformation.replaceMissingNumericalValues(column, attribute);


    assertEquals(122, attribute.getValue());


}

@Test
public void testReplaceMissingNumericalValues_NumberType_NoMissingValues()
        throws ClassNotFoundException {
    List<DataAttributes<Integer>> column = new ArrayList<>();
    // Add some data to the column, with no null values.
    column.add(new DataAttributes<>("Age", 40,
            Integer.class.getSimpleName(), Integer.class));
    column.add(new DataAttributes<>("Height", 175,
            Integer.class.getSimpleName(), Integer.class));
    column.add(new DataAttributes<>("Weight", 70,
            Integer.class.getSimpleName(), Integer.class));
    DataAttributes<Integer> attribute = new DataAttributes<>("Age", 40,
            Integer.class.getSimpleName(), Integer.class);
    dataTransformation.replaceMissingNumericalValues(column, attribute);

    //Assert
    assertEquals(40, attribute.getValue());
    assertEquals("Integer", attribute.getType());
    assertEquals("java.lang.Integer", attribute.getTypeClazzName());
}

    @Test
    public void testNormalizeData_NonNumberType() {
        assertThrows(ArithmeticException.class, () -> {
            List<DataAttributes<String>> column = new ArrayList<>();
            // Add some data to the column.
            DataAttributes<String> attribute = new DataAttributes<>("Name",
                    "John", "String", String.class);
            dataTransformation.normalizeData(column, attribute);
        });
    }

    @Test
    public void testNormalizeData_NumberType_MissingValues() throws ClassNotFoundException {
        List<DataAttributes<Double>> column = new ArrayList<>();
        // Add some data to the column, including null values.
        column.add(new DataAttributes<>("Height", null,
                "Double", Double.class));
        column.add(new DataAttributes<>("Weight", 70.0,
                "Double", Double.class));
        DataAttributes<Double> attribute = new DataAttributes<>("Height",
                null, "Double", Double.class);
        dataTransformation.normalizeData(column, attribute);

        assertNull(attribute.getValue());
    }

    @Test
    public void testNormalizeData_NumberType_NoMissingValues() throws ClassNotFoundException {
        List<DataAttributes<Double>> column = new ArrayList<>();
        // Add some data to the column, with no null values.
        column.add(new DataAttributes<>("Height", 175.0,
                "Double", Double.class));
        column.add(new DataAttributes<>("Weight", 70.0,
                "Double", Double.class));
        DataAttributes<Double> attribute = new DataAttributes<>("Height",
                175.0, "Double", Double.class);
        dataTransformation.normalizeData(column, attribute);
        log.info("Attribute value: {}", attribute.getValue());

        assertNotNull(attribute.getValue());
        assertTrue(true);
        double mean = column.stream().mapToDouble(DataAttributes::getValue).average().getAsDouble();
        double stdDev = Math.sqrt(column.stream().mapToDouble(DataAttributes::getValue)
                .map(v -> Math.pow(v - mean, 2)).average().getAsDouble());
        log.info("Mean: {}", mean);
        log.info("Std Dev: {}", stdDev);
        log.info("Normalized Value: {}", attribute.getValue());

        double normalizedValue = (attribute.getValue() - mean) / stdDev;
        assertEquals(normalizedValue, attribute.getValue());
        assertEquals(0, mean, 0.01);
        assertEquals(1, stdDev, 0.01);
        assertEquals("Double", attribute.getType());
        assertEquals("java.lang.Double", attribute.getTypeClazzName());
        assertEquals(2, column.size());


    }

    @Test
    public void testRemoveOutliers() {
        // Create a list of DataAttributes with some outlier values
        List<DataAttributes<Double>> column = new ArrayList<>();
        column.add(new DataAttributes<>("Height",
                175.0, "Double", Double.class));
        column.add(new DataAttributes<>("Height",
                180.0, "Double", Double.class));
        column.add(new DataAttributes<>("Height",
                185.0, "Double", Double.class));
        column.add(new DataAttributes<>("Height",
                190.0, "Double", Double.class));
        column.add(new DataAttributes<>("Height",
                300.0, "Double", Double.class)); // Outlier
        column.add(new DataAttributes<>("Height",
                -100.0, "Double", Double.class)); // Outlier

        // Call removeOutliers
        dataTransformation.removeOutliers(column);

        // Check that the outliers have been removed
        assertEquals(4, column.size());
        for (DataAttributes<Double> attr : column) {
            double value = attr.getValue();
            assert (value >= 175.0 && value <= 190.0);
        }
    }





}
