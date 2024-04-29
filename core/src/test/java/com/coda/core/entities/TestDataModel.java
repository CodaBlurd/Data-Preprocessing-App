package com.coda.core.entities;

import com.coda.core.entities.DataModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import java.util.Map;

public class TestDataModel {
    private DataModel<Object> dataModel;

    @BeforeEach
    public void setUp() {
        Map<String, DataAttributes<Object>> attributesMap = Map.of("test_attribute", new DataAttributes<>("test_attribute", "test_value", "test_type",
                Object.class));

        dataModel = new DataModel<>("test_id", attributesMap);
    }

    @Test
    public void testGetId() {
        String id = dataModel.getId();
        Assertions.assertEquals("test_id", id, "IDs should match");
    }

    @Test
    public void testGetAttributesMap() {
        Map<String, DataAttributes<Object>> attributesMap = dataModel.getAttributesMap();
        Assertions.assertNotNull(attributesMap, "Attribute map should not be null");

        DataAttributes<Object> da = attributesMap.get("test_attribute");
        Assertions.assertNotNull(da, "test_attribute should exist in the attributes map");
        Assertions.assertEquals("test_value", da.getValue(), "Values should match");
    }

    // Add more tests...
}      