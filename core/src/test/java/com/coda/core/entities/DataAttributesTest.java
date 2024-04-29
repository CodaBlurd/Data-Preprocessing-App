package com.coda.core.entities;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataAttributesTest {

    private DataAttributes<String> stringAttributes;
    private DataAttributes<Integer> integerAttributes;

    @BeforeEach
    void setUp() {
        stringAttributes = new DataAttributes<>(
                "name",
                "Value",
                "String",
                String.class);
        stringAttributes.setValidationRules("non-empty");
        stringAttributes.setRequired(true);

        integerAttributes = new DataAttributes<>(
                "age",
                25,
                "Integer",
                Integer.class);
        integerAttributes.setValidationRules("non-negative");
        integerAttributes.setRequired(true);
    }

    @Test
    void testTransformValue() {
        assertEquals("Value", stringAttributes.transformValue(), "Transformed values should match for strings");
        assertEquals(25, integerAttributes.transformValue(), "Transformed values should match for integers");
    }

    @Test
    void testDefaultValue() {
        stringAttributes.setDefaultValue("default");
        stringAttributes.setValue(null);
        stringAttributes.applyDefaultValue();
        assertEquals("default", stringAttributes.getValue(), "Value should be set to default when null");

        integerAttributes.setDefaultValue(0);
        integerAttributes.setValue(null);
        integerAttributes.applyDefaultValue();
        assertEquals(0, integerAttributes.getValue(), "Integer value should be set to default when null");
    }

    @Test
    void testApplyValidationRules() {
        assertTrue(stringAttributes.applyValidationRules(), "Validation should pass for non-empty string");
        stringAttributes.setValue("");
        assertFalse(stringAttributes.applyValidationRules(), "Validation should fail for empty string");

        assertTrue(integerAttributes.applyValidationRules(), "Validation should pass for non-negative integer");
        integerAttributes.setValue(-1);
        assertFalse(integerAttributes.applyValidationRules(), "Validation should fail for negative integer");
    }
}
