package com.coda.core.util.transform;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public final class IntegerTransform implements TransformValue {
    @Override
    public <T> Optional<T> transformValue(
            final String value, final Class<T> typeClazz,
            final String format) {
        try {
            if (value != null && !value.trim().isEmpty()
                    && Integer.class.equals(typeClazz)) {
                return Optional.of(
                        typeClazz.cast(
                                Integer.parseInt(value.trim())));
            }
        } catch (NumberFormatException e) {
            log.error("Error parsing value: {}, Message: {}, Cause: {}",
                    value, e.getMessage(), e.getCause(), e);
        }
        return Optional.empty();
    }
}
