package com.coda.core.util.transform;

import lombok.extern.slf4j.Slf4j;

import java.util.Optional;

@Slf4j
public final class DoubleTransform implements TransformValue {
    @Override
    public <T> Optional<T> transformValue(
            final String value, final Class<T> typeClazz,
            final String format) {
        // Check if the value is not null,
        // not empty, and typeClazz is exactly Double.class
        if (value != null && !value.trim().isEmpty()
                && Double.class.equals(typeClazz)) {
            try {
                Double parsedValue
                        = Double.parseDouble(value.trim());
                return Optional.of(typeClazz.cast(parsedValue));
            } catch (NumberFormatException e) {
                log.error("Error parsing value: {},"
                                + " Message: {}, Cause: {}",
                        value, e.getMessage(), e.getCause(), e);

            }
        }
        return Optional.empty();
    }
}
