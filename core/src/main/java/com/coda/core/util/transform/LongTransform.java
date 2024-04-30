package com.coda.core.util.transform;

import java.util.Optional;

public class LongTransform implements TransformValue{
    @Override
    public <T> Optional<T> transformValue(String value, Class<T> typeClazz, String format) {
        try {
            // Ensure value is not null or empty and typeClazz is exactly Long.class
            if (value != null && !value.trim().isEmpty() && Long.class.equals(typeClazz)) {
                // Parse the long from the trimmed string and cast to type T safely
                return Optional.of(typeClazz.cast(Long.parseLong(value.trim())));
            }
        } catch (NumberFormatException e) {
            // If the value is not a valid long, return empty Optional
            return Optional.empty();
        }
        return Optional.empty();
    }
}
