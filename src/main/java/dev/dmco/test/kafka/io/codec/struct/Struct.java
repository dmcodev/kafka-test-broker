package dev.dmco.test.kafka.io.codec.struct;

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
public class Struct {

    private final Collection<StructEntry> fields;
    private final Constructor<?> constructor;

    public Struct(Class<?> type) {
        fields = Arrays.stream(type.getDeclaredFields())
            .map(StructEntry::from)
            .collect(Collectors.toList());
        constructor = type.getConstructors()[0];
    }
}
