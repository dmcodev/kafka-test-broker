package dev.dmco.test.kafka.io.codec.context;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ContextProperty<T> {

    public static final ContextProperty<Integer> API_VERSION = new ContextProperty<>("API_VERSION");
    public static final ContextProperty<Boolean> EXCLUDE_FIELD = new ContextProperty<>("EXCLUDE_FIELD");

    private final String name;

    @Override
    public String toString() {
        return name;
    }
}
