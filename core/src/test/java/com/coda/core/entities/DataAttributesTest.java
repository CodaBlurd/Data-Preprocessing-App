package com.coda.core.entities;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

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
        // Assuming the transformation does not alter the input for strings
        Optional<String> stringOptional = stringAttributes.transformValue();
        assertTrue(stringOptional.isPresent(), "String value should be transformed");
        assertEquals("Value", stringOptional.get(), "Transformed string value should match input");

        // Assuming no actual transformation logic alters the integer
        Optional<Integer> integerOptional = integerAttributes.transformValue();
        assertTrue(integerOptional.isPresent(), "Integer value should be transformed");
        assertEquals(25, integerOptional.get(), "Transformed integer value should match input");
    }


    @Test
    void testDefaultValue() {
        stringAttributes.setDefaultValue("default");
        stringAttributes.setValue(Optional.empty());
        stringAttributes.applyDefaultValue();
        assertEquals("default", stringAttributes.getValue(), "Value should be set to default when null");

        integerAttributes.setDefaultValue(0);
        integerAttributes.setValue(Optional.empty());
        integerAttributes.applyDefaultValue();
        assertEquals(0, integerAttributes.getValue(), "Integer value should be set to default when null");
    }

    @Test
    void testApplyValidationRules() {
        assertTrue(stringAttributes.applyValidationRules(), "Validation should pass for non-empty string");
        stringAttributes.setValue(Optional.of(""));
        assertFalse(stringAttributes.applyValidationRules(), "Validation should fail for empty string");

        assertTrue(integerAttributes.applyValidationRules(), "Validation should pass for non-negative integer");
        integerAttributes.setValue(Optional.of(-1));
        assertFalse(integerAttributes.applyValidationRules(), "Validation should fail for negative integer");
    }
}
