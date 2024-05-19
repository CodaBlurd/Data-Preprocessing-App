package com.coda.core.util.transform;

import java.util.Optional;

public final class BooleanTransform
        implements TransformValue {

    /**
     * Transforms a string value to a Boolean value.
     * @param value     the value to transform
     * @param typeClazz the type to transform to
     *                  i.e. Boolean.class
     * @param format    the format to use for transformation
     *                  i.e. true or false
     * @return        an Optional of the transformed value
     * @param <T>     the type to transform to.
     */

    @Override
    public <T> Optional<T> transformValue(
            final String value, final Class<T> typeClazz,
            final String format) {
        // Ensure value is not null or empty
        // and typeClazz is exactly Boolean.class
        if (value != null && !value.trim().isEmpty()
                && Boolean.class.equals(typeClazz)) {
            // Parse the boolean from the
            // trimmed string and cast to type T safely
            return Optional.of(
                    typeClazz.cast(
                            Boolean.parseBoolean(value.trim())));
        }
        return Optional.empty();
    }
}
