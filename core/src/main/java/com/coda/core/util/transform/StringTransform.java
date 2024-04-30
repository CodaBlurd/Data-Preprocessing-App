package com.coda.core.util.transform;

import java.util.Optional;

public class StringTransform implements TransformValue {
    @Override
    public <T> Optional<T> transformValue(String value, Class<T> typeClazz, String format) {
        // First, check if the type class is String
        if (String.class.equals(typeClazz)) {
            // If value is not null and not empty, return it cast to T
            if (value != null && !value.trim().isEmpty()) {
                return Optional.of(typeClazz.cast(value));
            }
            // If the string is empty or null, return an empty Optional
            return Optional.empty();
        }
        // If typeClazz is not String, return empty as no transformation is applicable
        return Optional.empty();
    }
}
