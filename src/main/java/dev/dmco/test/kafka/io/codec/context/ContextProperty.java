package dev.dmco.test.kafka.io.codec.context;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ContextProperty<T> {

    public static final ContextProperty<Integer> VERSION = new ContextProperty<>("VERSION");
    public static final ContextProperty<Boolean> VERSION_MISMATCH = new ContextProperty<>("VERSION_MISMATCH");

    private final String name;

    @Override
    public String toString() {
        return name;
    }
}
