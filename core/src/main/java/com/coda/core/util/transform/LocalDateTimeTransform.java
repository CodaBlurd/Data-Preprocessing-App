package com.coda.core.util.transform;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

@Slf4j
public final class LocalDateTimeTransform implements TransformValue {
    @Override
    public <T> Optional<T> transformValue(
            final String value, final Class<T> typeClazz, final String format) {
        try {
            if (value != null && !value.trim().isEmpty()
                    && LocalDateTime.class.equals(typeClazz)) {
                if (format == null || format.isEmpty()) {
                    return Optional.empty();
                }
                // Use trim and check format
                DateTimeFormatter formatter
                        = DateTimeFormatter.ofPattern(format);
                LocalDateTime dateTime
                        = LocalDateTime.parse(value.trim(), formatter);
                return Optional.of(typeClazz.cast(dateTime));
            }
        } catch (DateTimeParseException e) {
            log.error(
                    "Error parsing LocalDateTime from value: {}",
                    value, e);
        }
        return Optional.empty();
    }
}
