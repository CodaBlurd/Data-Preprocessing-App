package com.coda.core.util.transform;

import java.util.Optional;

public class ObjectTransform implements TransformValue {
    @Override
    public <T> Optional<T> transformValue(String value, Class<T> typeClazz, String format) {
        // Check if the value is not null and not empty
        if (value == null || value.trim().isEmpty()) {
            return Optional.empty();
        }
        // If the expected type is Object, return the value cast to T
        if (Object.class.equals(typeClazz)) {
            return Optional.of(typeClazz.cast(value));
        }
        // If type is not Object, handle it or simply return empty Optional
        return Optional.empty();
    }
}
