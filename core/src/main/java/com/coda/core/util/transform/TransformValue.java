package com.coda.core.util.transform;

import java.util.Optional;

public interface TransformValue {
    <T> Optional<T> transformValue(String value, Class<T> typeClazz, String format);
}
