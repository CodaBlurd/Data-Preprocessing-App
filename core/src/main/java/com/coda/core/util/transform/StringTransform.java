package com.coda.core.util.transform;

import java.util.Optional;

public final class StringTransform implements TransformValue {

    /**
     * Transforms the given value to the specified type.
     * @param value     the value to transform
     * @param typeClazz the type to transform to
     *                  i.e. String.class
     * @param format    the format to use for transformation
     *                  i.e. "name"; "address"; etc.
     * @return an optional of the transformed value
     * @param <T>      the type to transform to.
     */
    @Override
    public <T> Optional<T> transformValue(
            final String value, final Class<T> typeClazz,
            final String format) {
        // First, check if the type class is String
        if (String.class.equals(typeClazz)) {
            // If value is not null and not empty, return it cast to T
            if (value != null && !value.trim().isEmpty()) {
                return Optional.of(typeClazz.cast(value));
            }
            // If the string is empty or null, return an empty Optional
            return Optional.empty();
        }
        // If typeClazz is not String,
        // return empty as no transformation is applicable
        return Optional.empty();
    }
}

