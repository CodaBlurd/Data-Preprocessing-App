package com.coda.core.util.transform;

import java.util.Optional;

public final class ObjectTransform implements TransformValue {

    /**
     * Transforms a string value to an Object value.
     * @param value     the value to transform
     * @param typeClazz the type to transform to
     *                  i.e. Object.class.
     * @param format    the format to use for transformation.
     *                  i.e. null.
     * @return         an Optional of the transformed value.
     * @param <T>      the type to transform to.
     */
    @Override
    public <T> Optional<T> transformValue(
            final String value, final Class<T> typeClazz,
            final String format) {
        // Check if the value is
        // not null and not empty
        if (value == null
                || value.trim().isEmpty()) {
            return Optional.empty();
        }
        // If the expected type is Object,
        // return the value cast to T
        if (Object.class.equals(typeClazz)) {
            return Optional.of(typeClazz.cast(value));
        }
        // If type is not Object,
        // handle it or simply return empty Optional
        return Optional.empty();
    }
}
