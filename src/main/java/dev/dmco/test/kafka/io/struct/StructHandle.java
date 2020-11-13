package dev.dmco.test.kafka.io.struct;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

@Getter
@RequiredArgsConstructor
@Accessors(fluent = true)
public class StructHandle {

    private final Collection<FieldHandle> fields;
    private final Constructor<?> constructor;

    public StructHandle(Class<?> type) {
        fields = Arrays.stream(type.getDeclaredFields())
            .map(FieldHandle::from)
            .collect(Collectors.toList());
        constructor = type.getConstructors()[0];
    }
}
