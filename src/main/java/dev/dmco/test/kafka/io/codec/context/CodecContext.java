package dev.dmco.test.kafka.io.codec.context;

import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RequiredArgsConstructor
public class CodecContext {

    private final Map<ContextProperty<?>, Object> properties;

    public CodecContext() {
        this(new HashMap<>());
    }

    @SuppressWarnings("unchecked")
    public <T> T get(ContextProperty<T> property) {
        return (T) properties.get(property);
    }

    public <T> T getOrDefault(ContextProperty<T> property, T defaultValue) {
        return Optional.ofNullable(get(property))
            .orElse(defaultValue);
    }

    public CodecContext set(ContextProperty<?> property, Object value) {
        Map<ContextProperty<?>, Object> newProperties = new HashMap<>(properties);
        newProperties.put(property, value);
        return new CodecContext(newProperties);
    }

    public CodecContext merge(CodecContext another) {
        Map<ContextProperty<?>, Object> newProperties = new HashMap<>(properties);
        newProperties.putAll(another.properties);
        return new CodecContext(newProperties);
    }
}
