package com.coda.core.util.transform;

import java.util.Optional;

public class DoubleTransform implements TransformValue {
    @Override
    public <T> Optional<T> transformValue(String value, Class<T> typeClazz, String format) {
        // Check if the value is not null, not empty, and typeClazz is exactly Double.class
        if (value != null && !value.trim().isEmpty() && Double.class.equals(typeClazz)) {
            try {
                Double parsedValue = Double.parseDouble(value.trim());
                return Optional.of(typeClazz.cast(parsedValue));
            } catch (NumberFormatException e) {
                // Optionally log the error or handle it according to your needs
                System.err.println("Error parsing double from value: " + value + " - " + e.getMessage());
            }
        }
        return Optional.empty();
    }
}
