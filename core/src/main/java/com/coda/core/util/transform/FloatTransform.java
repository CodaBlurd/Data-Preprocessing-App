package com.coda.core.util.transform;

import java.util.Optional;

public class FloatTransform implements TransformValue{
    @Override
    public <T> Optional<T> transformValue(String value, Class<T> typeClazz, String format) {
        // Ensure value is not null or empty and typeClazz is exactly Float.class
        if (value != null && !value.trim().isEmpty() && Float.class.equals(typeClazz)) {
            // Parse the float from the trimmed string and cast to type T safely
            return Optional.of(typeClazz.cast(Float.parseFloat(value.trim())));
        }
        return Optional.empty();
    }
}
