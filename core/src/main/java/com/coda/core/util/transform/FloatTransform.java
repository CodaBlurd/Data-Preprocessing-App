package com.coda.core.util.transform;

import java.util.Optional;

public class FloatTransform
        implements TransformValue {

    /**
     * Transforms the given value to the specified type.
     * @param value     the value to transform
     * @param typeClazz the type to transform to
     *                  i.e. Float.class
     * @param format    the format to use for transformation
     *                  i.e. "0.00"
     * @return an optional of the transformed value
     * @param <T>      the type to transform to.
     */
    @Override
    public <T> Optional<T> transformValue(
            final String value, final Class<T> typeClazz,
            final String format) {
        // Ensure value is not null
        // or empty and typeClazz is exactly Float.class
        if (value != null && !value.trim().isEmpty()
                && Float.class.equals(typeClazz)) {
            // Parse the float from the
            // trimmed string and cast to type T safely
            return Optional.of(typeClazz.cast(
                    Float.parseFloat(value.trim())));
        }
        return Optional.empty();
    }
}

