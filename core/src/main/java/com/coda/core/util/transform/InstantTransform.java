package com.coda.core.util.transform;

import java.time.Instant;
import java.util.Optional;

public final class InstantTransform
        implements TransformValue {

    /**
     * Transforms the given value to the given type.
     * @param value     the value to transform
     * @param typeClazz the type to transform to
     *                  i.e. Instant.class
     * @param format    the format to use for transformation
     *                  i.e. instant format "yyyy-MM-dd'T'HH:mm:ss'Z'"
     * @return an Optional of the transformed value
     * @param <T> the type to transform to.
     */

    @Override
    public <T> Optional<T> transformValue(
            final String value,
            final Class<T> typeClazz,
            final String format) {
        // Ensure value is not null
        // or empty and typeClazz is exactly Instant.class
        if (value != null && !value.trim().isEmpty()
                && Instant.class.equals(typeClazz)) {
            // Parse the Instant from the
            // trimmed string and cast to type T safely
            return Optional.of(typeClazz.cast(
                    Instant.parse(value.trim())));
        }
        return Optional.empty();
    }
}
