package com.coda.core.util.transform;

import java.time.Instant;
import java.util.Optional;

public class InstantTransform implements TransformValue{
    @Override
    public <T> Optional<T> transformValue(String value, Class<T> typeClazz, String format) {
        // Ensure value is not null or empty and typeClazz is exactly Instant.class
        if (value != null && !value.trim().isEmpty() && Instant.class.equals(typeClazz)) {
            // Parse the Instant from the trimmed string and cast to type T safely
            return Optional.of(typeClazz.cast(Instant.parse(value.trim())));
        }
        return Optional.empty();
    }
}
