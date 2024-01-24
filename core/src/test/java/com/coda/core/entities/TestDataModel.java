package com.coda.core.entities;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Map;

/**
 * This class is used to test the DataModel class
 * <p> This class is responsible for testing the DataModel class</p>
 * {@code @BeforeEach} annotation to run the setUp method before each test
 * {@code @Test} annotation to mark the method as a test method
 */

public class TestDataModel {
    private DataModel<Object> dataModel;
    private DataAttributes<Object> dataAttributes;
    private DataModel<Object> dataModel2;
    private DataAttributes<Object> dataAttributes2;
    private Map<String, DataModel<Object>> data;

    @BeforeEach
    public void setUp() {
        // Arrange
        dataModel = new DataModel<>(
                "test_id",
                Map.of("test_attribute", new DataAttributes<>("test_attribute", "test_value", "test_type"))
        );
        dataAttributes = new DataAttributes<>("test_attribute", "test_value", "test_type");
        dataModel2 = new DataModel<>(
                "test_id",
                Map.of("test_attribute", new DataAttributes<>("test_attribute", "test_value", "test_type"))
        );




    }


    @Test
    public void testTestDataModel() {
        // Arrange
        dataModel = new DataModel<>(
                "test_id",
                Map.of("test_attribute", new DataAttributes<>("test_attribute", "test_value", "test_type"))
        );
        dataAttributes = new DataAttributes<>("test_attribute", "test_value", "test_type");
        dataModel2 = new DataModel<>(
                "test_id",
                Map.of("test_attribute", new DataAttributes<>("test_attribute", "test_value", "test_type"))
        );
        dataAttributes2 = new DataAttributes<>("test_attribute", "test_value", "test_type");
        data = Map.of("test_id", dataModel);

        // Act
        dataModel.setId("test_id");
        dataModel.setAttributesMap(Map.of("test_attribute", new DataAttributes<>("test_attribute", "test_value", "test_type")));
        dataModel2.setId("test_id");
        dataModel2.setAttributesMap(Map.of("test_attribute", new DataAttributes<>("test_attribute", "test_value", "test_type")));

    }
}
