package com.coda.core.util.transform;

import java.util.Optional;

public final class LongTransform implements TransformValue {

    /**
     * Transforms a string value to a Long value.
     * @param value     the value to transform
     * @param typeClazz the type to transform to
     *                  i.e. Long.class.
     * @param format    the format to use for transformation.
     *                  i.e. 2L.
     * @return         an Optional of the transformed value.
     * @param <T>      the type to transform to.
     */
    @Override
    public <T> Optional<T> transformValue(
            final String value, final Class<T> typeClazz,
            final String format) {
        try {
            // Ensure value is not null
            // or empty and typeClazz is exactly Long.class.
            if (value != null && !value.trim().isEmpty()
                    && Long.class.equals(typeClazz)) {
                // Parse the long from
                // the trimmed string and cast to type T safely.
                return Optional.of(
                        typeClazz.cast(Long.parseLong(
                                value.trim())));
            }
        } catch (NumberFormatException e) {
            // If the value is not a valid long, return empty Optional.
            return Optional.empty();
        }
        return Optional.empty();
    }
}
