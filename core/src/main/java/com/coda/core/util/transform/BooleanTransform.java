package com.coda.core.util.transform;

import java.util.Optional;

public class BooleanTransform implements TransformValue {
    @Override
    public <T> Optional<T> transformValue(String value, Class<T> typeClazz, String format) {
        // Ensure value is not null or empty and typeClazz is exactly Boolean.class
        if (value != null && !value.trim().isEmpty() && Boolean.class.equals(typeClazz)) {
            // Parse the boolean from the trimmed string and cast to type T safely
            return Optional.of(typeClazz.cast(Boolean.parseBoolean(value.trim())));
        }
        return Optional.empty();
    }
}
