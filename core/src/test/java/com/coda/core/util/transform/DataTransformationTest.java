package com.coda.core.util.transform;

import com.coda.core.entities.DataAttributes;
import com.coda.core.entities.DataModel;
import com.coda.core.exceptions.TransformationException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

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
            dataTransformation.transformValue("java.lang.Integer",
                    null,
                    "", null);
        });
    }

    @Test
    public void testTransformValue_NoTransformationStrategy() {
        assertThrows(TransformationException.class, () -> {
            dataTransformation.transformValue("UnknownType", "123",
                    "java.lang.Integer", null);
        });
    }

    @Test
    public void testTransformValue_Success() {
        Integer transformedValue = dataTransformation.transformValue("java.lang.Integer",
                "123", null, "age");
        assertEquals(123, transformedValue);
    }

    @Test
    public void testCleanCategoricalValues_NonStringType() throws ClassNotFoundException {
        Object value = dataTransformation.cleanCategoricalValues("java.lang.Integer",
                "123");
        assertEquals("123", value);
    }

    @Test
    public void testCleanCategoricalValues_NonStringValue() throws ClassNotFoundException {
        Object value = dataTransformation.cleanCategoricalValues("java.lang.String", 123);
        assertEquals(123, value);
    }

    @Test
    public void testCleanCategoricalValues_StringValue() throws ClassNotFoundException {
        String value = dataTransformation.cleanCategoricalValues("java.lang.String",
                "Hello, World!");
        assertEquals("Hello, World!", value);
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
        column.add(new DataAttributes<>("Name", null,
                "Java.lang.String", String.class));
        column.add(new DataAttributes<>("Age", "30",
                "java.lang.String", String.class));
        column.add(new DataAttributes<>("Address", null,
                "java.lang.String", String.class));

        for (DataAttributes<String> attribute : column) {
            log.info("Before: {}", attribute.getValue());
            log.info("Type: {}", attribute.getType());
            log.info("Name: {}", attribute.getAttributeName());
            log.info("Class: {}", attribute.getTypeClazzName());
            log.info("Format: {}", attribute.getFormat());
            log.info("----------");

        }

        dataTransformation.replaceMissingCategoricalValues(column, "java.lang.String");


        // Add assertions here to verify the behavior of the replaceMissingCategoricalValues method.
        assertEquals(3, column.size());

        for (DataAttributes<String> attribute : column) {
            log.info("After: {}", attribute.getValue());
            log.info("Type: {}", attribute.getType());
            log.info("Name: {}", attribute.getAttributeName());
            log.info("Class: {}", attribute.getTypeClazzName());
            log.info("Format: {}", attribute.getFormat());
            log.info("----------");
        }
    }

    @Test
    public void testReplaceMissingCategoricalValues_StringType_NoMissingValues() {
        List<DataAttributes<String>> column = new ArrayList<>();
        // Add some data to the column, without null values.
        column.add(new DataAttributes<>("Name", "John",
                "java.lang.String", String.class));
        column.add(new DataAttributes<>("Age", "30",
                "java.lang.String", String.class));
        // Add some data to the column, with no null values.
        dataTransformation.replaceMissingCategoricalValues(column, "java.lang.String");
        // Add assertions here to verify the behavior of the replaceMissingCategoricalValues method.
        assertEquals(2, column.size());
        assertEquals("John", column.get(0).getValue());
        assertEquals("30", column.get(1).getValue());
        assertEquals("java.lang.String", column.get(0).getType());

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
    public void testNormalizeData_NumberType_MissingValues() throws ClassNotFoundException {
        List<DataModel<Double>> dataModels = new ArrayList<>();
        DataModel<Double> dataModel = new DataModel<>();
        DataModel<Double> dataModel1 = new DataModel<>();
        Map<String, DataAttributes<Double>> attributesMap = new HashMap<>();
        Map<String, DataAttributes<Double>> attributesMap1 = new HashMap<>();

        DataAttributes<Double> attribute = new DataAttributes<>("Height", null, "java.lang.Double", Double.class);
        DataAttributes<Double> attribute1 = new DataAttributes<>("Weight", 100.0, "java.lang.Double", Double.class);
        attributesMap.put("Height", attribute);
        attributesMap.put("Weight", attribute1);
        dataModel.setAttributesMap(attributesMap);

        DataAttributes<Double> attr = new DataAttributes<>("Height", null, "java.lang.Double", Double.class);
        DataAttributes<Double> attr1 = new DataAttributes<>("Weight", 75.0, "java.lang.Double", Double.class);
        attributesMap1.put("Height", attr);
        attributesMap1.put("Weight", attr1);
        dataModel1.setAttributesMap(attributesMap1);

        dataModels.add(dataModel);
        dataModels.add(dataModel1);
        dataTransformation.normalize(dataModels);

        // Add assertions here to verify the behavior of the normalize method.
        assertEquals(2, dataModels.size());
        assertEquals("java.lang.Double",
                dataModels.get(0).getAttributesMap().get("Height").getType());
    }

    @Test
    public void testNormalizeData_NumberType_NoMissingValues() throws ClassNotFoundException {
        List<DataModel<Double>> dataModels = new ArrayList<>();
        DataModel<Double> dataModel = new DataModel<>();
        DataModel<Double> dataModel1 = new DataModel<>();
        Map<String, DataAttributes<Double>> attributesMap = new HashMap<>();
        Map<String, DataAttributes<Double>> attributesMap1 = new HashMap<>();

        DataAttributes<Double> attribute = new DataAttributes<>("Height", 175.0, "java.lang.Double", Double.class);
        DataAttributes<Double> attribute1 = new DataAttributes<>("Weight", 70.0, "java.lang.Double", Double.class);
        attributesMap.put("Height", attribute);
        attributesMap.put("Weight", attribute1);
        dataModel.setAttributesMap(attributesMap);

        DataAttributes<Double> attr = new DataAttributes<>("Height", 183.0, "java.lang.Double", Double.class);
        DataAttributes<Double> attr1 = new DataAttributes<>("Weight", 89.0, "java.lang.Double", Double.class);
        attributesMap1.put("Height", attr);
        attributesMap1.put("Weight", attr1);
        dataModel1.setAttributesMap(attributesMap1);

        dataModels.add(dataModel);
        dataModels.add(dataModel1);
        dataTransformation.normalize(dataModels);

        assertNotNull(attribute.getValue());
        assertNotNull(attr.getValue());

        double meanHeight = (175.0 + 183.0) / 2;
        double stdDevHeight = Math.sqrt((Math.pow(175.0 - meanHeight, 2)
                + Math.pow(183.0 - meanHeight, 2)) / 2);

        double normalizedHeight1 = (175.0 - meanHeight) / stdDevHeight;
        double normalizedHeight2 = (183.0 - meanHeight) / stdDevHeight;

        // Debug statements to check actual values
        System.out.println("Expected normalizedHeight1: " + normalizedHeight1);
        System.out.println("Expected normalizedHeight2: " + normalizedHeight2);
        System.out.println("Actual normalizedHeight1: " + dataModels.get(0).getAttributesMap().get("Height").getValue());
        System.out.println("Actual normalizedHeight2: " + dataModels.get(1).getAttributesMap().get("Height").getValue());

        assertEquals(normalizedHeight1, (Double) dataModels.get(0).getAttributesMap().get("Height").getValue(), 0.0001);
        assertEquals(normalizedHeight2, (Double) dataModels.get(1).getAttributesMap().get("Height").getValue(), 0.0001);
        assertEquals("java.lang.Double", attribute.getType());
    }

//    @Test
//    public void testEncodeCategoricalVariable() {
//        List<DataModel<String>> dataModels = new ArrayList<>();
//        DataModel<String> dataModel = new DataModel<>();
//        DataModel<String> dataModel1 = new DataModel<>();
//        Map<String, DataAttributes<String>> attributesMap = new HashMap<>();
//        Map<String, DataAttributes<String>> attributesMap1 = new HashMap<>();
//
//        attributesMap.put("Department", new DataAttributes<>("Department", "Engineering", "java.lang.String", String.class));
//        attributesMap.put("Age", new DataAttributes<>("Age", "30", "java.lang.String", String.class));
//        attributesMap.put("Address", new DataAttributes<>("Address", "123 Main St", "java.lang.String", String.class));
//        dataModel.setAttributesMap(attributesMap);
//
//        attributesMap1.put("Department", new DataAttributes<>("Department", "HR", "java.lang.String", String.class));
//        attributesMap1.put("Age", new DataAttributes<>("Age", "40", "java.lang.String", String.class));
//        attributesMap1.put("Address", new DataAttributes<>("Address", "456 Elm St", "java.lang.String", String.class));
//        dataModel1.setAttributesMap(attributesMap1);
//
//        dataModels.add(dataModel);
//        dataModels.add(dataModel1);
//
//        Set<String> categoricalAttributes = new HashSet<>(List.of("Department"));
//
//        dataTransformation.encodeCatVariables(dataModels, categoricalAttributes);
//
//        // Add assertions here to verify the behavior of the encodeCatVariables method.
//        assertEquals(2, dataModels.size());
//
//        DataAttributes<String> departmentAttr1 = dataModels.get(0).getAttributesMap().get("Department");
//        DataAttributes<String> departmentAttr2 = dataModels.get(1).getAttributesMap().get("Department");
//        log.info(departmentAttr1.getEncodedValues().toString());
//
//        assertNotNull(departmentAttr1.getEncodedValues());
//        assertNotNull(departmentAttr2.getEncodedValues());
//
//        Map<String, Integer> encodedDept1 = departmentAttr1.getEncodedValues();
//        Map<String, Integer> encodedDept2 = departmentAttr2.getEncodedValues();
//
//        assertEquals(1, encodedDept1.get("Engineering").intValue());
//        assertEquals(0, encodedDept1.get("HR").intValue());
//
//        assertEquals(0, encodedDept2.get("Engineering").intValue());
//        assertEquals(1, encodedDept2.get("HR").intValue());
//    }



    @Test
    void testEncodeCatVariables() {
        // Setup
        List<DataModel<String>> dataModels = new ArrayList<>();
        DataModel<String> model1 = new DataModel<>();
        Map<String, DataAttributes<String>> attributesMap1 = new HashMap<>();
        attributesMap1.put("Department", new DataAttributes<>("Department",
                "Engineering", "java.lang.String", String.class));
        model1.setAttributesMap(attributesMap1);

        DataModel<String> model2 = new DataModel<>();
        Map<String, DataAttributes<String>> attributesMap2 = new HashMap<>();
        attributesMap2.put("Department", new DataAttributes<>("Department", "HR",
                "java.lang.String", String.class));
        model2.setAttributesMap(attributesMap2);

        dataModels.add(model1);
        dataModels.add(model2);

        Set<String> categoricalAttributes = new HashSet<>(Arrays.asList("Department"));

        // Invoke
        dataTransformation.encodeCatVariables(dataModels, categoricalAttributes);

        // Verify
        for (DataModel<String> model : dataModels) {
            Map<String, DataAttributes<String>> attributes = model.getAttributesMap();
            assertTrue(attributes.containsKey("Engineering") || attributes.containsKey("HR"),
                    "Encoded attributes should contain 'Engineering' or 'HR'.");
            assertEquals(2, attributes.size(),
                    "Each model should have two attributes after encoding.");
            assertNotNull(attributes.get("Engineering") != null ? attributes.get("Engineering").getValue() : attributes.get("HR").getValue(),
                    "Encoded value should not be null.");
        }
    }





}
