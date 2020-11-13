package dev.dmco.test.kafka.io.struct;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@RequiredArgsConstructor
@Accessors(fluent = true)
public class StructHandle {

    private final Map<Integer, Collection<FieldHandle>> fieldsByApiVersion = new HashMap<>();

    private final Collection<FieldHandle> fields;
    private final Constructor<?> constructor;

    public StructHandle(Class<?> type) {
        fields = Arrays.stream(type.getDeclaredFields())
            .map(FieldHandle::from)
            .collect(Collectors.toList());
        constructor = type.getConstructors()[0];
    }

    public Collection<FieldHandle> fieldsForApiVersion(int apiVersion) {
        return fieldsByApiVersion.computeIfAbsent(apiVersion, this::findFieldsForApiVersion);
    }

    private Collection<FieldHandle> findFieldsForApiVersion(int apiVersion) {
        return fields.stream()
            .filter(it -> it.sinceVersion() <= apiVersion)
            .collect(Collectors.toList());
    }
}
