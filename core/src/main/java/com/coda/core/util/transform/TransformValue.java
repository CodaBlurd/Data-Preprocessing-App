package com.coda.core.util.transform;

import java.util.Optional;

public interface TransformValue {

    /**
     * Transform value to the specified type.
     *
     * @param value     the value to transform
     * @param typeClazz the type to transform to
     *                  i.e. LocalDateTime.class
     * @param format    the format to use for transformation
     *                  i.e. "yyyy-MM-dd HH:mm:ss"
     * @param <T>       the type to transform to
     * @return the transformed value
     */
    <T> Optional<T> transformValue(
            String value, Class<T> typeClazz,
            String format);
}

