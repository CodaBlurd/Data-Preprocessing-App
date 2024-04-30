package com.coda.core.util.transform;

import java.util.Optional;

public class IntegerTransform implements TransformValue {
    @Override
    public <T> Optional<T> transformValue(String value, Class<T> typeClazz, String format) {
        try {
            if (value != null && !value.trim().isEmpty() && Integer.class.equals(typeClazz)) {
                return Optional.of(typeClazz.cast(Integer.parseInt(value.trim())));
            }
        } catch (NumberFormatException e) {
            // Log the exception or handle it according to your application's error handling policy
            System.out.println("Failed to parse integer: " + e.getMessage()); // Example logging
        }
        return Optional.empty();
    }
}
