package com.coda.core.entities;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
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
        Optional<String> stringOptional
                = stringAttributes.transformValue().describeConstable();
        assertTrue(stringOptional.isPresent(),
                "String value should be transformed");
        assertEquals("Value", stringOptional.get(),
                "Transformed string value should match input");

        // Assuming no actual transformation logic alters the integer
        Optional<Integer> integerOptional
                = integerAttributes.transformValue().describeConstable();
        assertTrue(integerOptional.isPresent(),
                "Integer value should be transformed");
        assertEquals(25, integerOptional.get(),
                "Transformed integer value should match input");
    }


    @Test
    void testDefaultValue() {
        DataAttributes<String> stringDataAttributes
                = new DataAttributes<>("testString", "validString",
                "String", String.class);
        log.info("Testing string attributes");
        log.info("Default value set to: {}",
                stringDataAttributes.getDefaultValue());
        stringDataAttributes.setValue(null);
        stringDataAttributes.setDefaultValue("default");
        stringDataAttributes.applyDefaultValue();

        log.info("Value after applyDefaultValue: {}",
                stringDataAttributes.getValue());
        assertEquals("default", stringDataAttributes.getValue(),
                "Value should be set to default when null");

        stringDataAttributes.setValue("");
        stringDataAttributes.setDefaultValue("default-1");
        stringDataAttributes.applyDefaultValue();
        log.info("Value after applyDefaultValue: {}",
                stringDataAttributes.getValue());
        assertEquals("default-1", stringDataAttributes.getValue(),
                "Value should be set to default when empty");

        log.info("Testing integer attributes");
        integerAttributes.setDefaultValue(0);
        log.info("Default value set to: {}",
                integerAttributes.getDefaultValue());
        integerAttributes.setValue(null);
        integerAttributes.applyDefaultValue();
        log.info("Value after applyDefaultValue: {}",
                integerAttributes.getValue());
        assertEquals(0, integerAttributes.getValue(),
                "Integer value should be set to default when null");
    }


    @Test
    void testApplyValidationRules() {
        // Test for String attributes
        DataAttributes<String> stringDataAttributes
                = new DataAttributes<>("testString", "validString",
                "String", String.class);
        stringDataAttributes.setValidationRules("non-empty");
        stringDataAttributes.setRequired(true);
        stringDataAttributes.initializeValidationRules();

        log.info("Validation rules (non-empty, validString): {}",
                stringDataAttributes.getValidationRulesList());
        assertTrue(stringDataAttributes.applyValidationRules(),
                "Validation should pass for non-empty string");

        stringDataAttributes.setValue("");
        log.info("Validation rules (non-empty, empty): {}",
                stringDataAttributes.getValidationRulesList());
        log.info("Value: '{}", stringDataAttributes.getValue() + "'");
        assertFalse(stringDataAttributes.applyValidationRules(),
                "Validation should fail for empty string");

        // Test for Integer attributes
        DataAttributes<Integer> integerAttributes
                = new DataAttributes<>("testInteger", 10,
                "Integer", Integer.class);
        integerAttributes.setValidationRules("non-negative");
        integerAttributes.setRequired(true);
        integerAttributes.initializeValidationRules();

        log.info("Validation rules (non-negative, 10): {}",
                integerAttributes.getValidationRulesList());
        assertTrue(integerAttributes.applyValidationRules(),
                "Validation should pass for non-negative integer");

        integerAttributes.setValue(-1);
       log.info("Validation rules (non-negative, -1): {}",
               integerAttributes.getValidationRulesList());
        assertFalse(integerAttributes.applyValidationRules(),
                "Validation should fail for negative integer");
    }

}
