package dev.dmco.test.kafka.io.codec.context;

import dev.dmco.test.kafka.io.codec.registry.TypeKey;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ContextProperty<T> {

    public static final ContextProperty<Integer> VERSION = new ContextProperty<>("API_VERSION");
    public static final ContextProperty<Boolean> EXCLUDE_FIELD = new ContextProperty<>("EXCLUDE_FIELD");
    public static final ContextProperty<TypeKey> CURRENT_TYPE_KEY = new ContextProperty<>("TYPE_KEY");

    private final String name;

    @Override
    public String toString() {
        return name;
    }
}
