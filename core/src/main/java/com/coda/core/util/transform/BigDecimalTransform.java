package com.coda.core.util.transform;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Optional;

@Slf4j
public class BigDecimalTransform implements TransformValue{
    @Override
    public <T> Optional<T> transformValue(String value, Class<T> typeClazz, String format) {
        if (value != null && !value.trim().isEmpty()
                && BigDecimal.class.equals(typeClazz)) {
            try {
                BigDecimal parsedValue
                        = new BigDecimal(value.trim());
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
