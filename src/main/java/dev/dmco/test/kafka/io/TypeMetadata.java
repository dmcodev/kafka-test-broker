package dev.dmco.test.kafka.io;

import lombok.Value;
import lombok.experimental.Accessors;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Value
@Accessors(fluent = true)
public class TypeMetadata {

    Map<Integer, Collection<TypeField>> fieldsByApiVersion = new HashMap<>();

    Collection<TypeField> fields;
    Constructor<?> constructor;

    public TypeMetadata(Class<?> type) {
        fields = Arrays.stream(type.getDeclaredFields())
            .map(TypeField::from)
            .collect(Collectors.toList());
        constructor = type.getConstructors()[0];
    }

    public Collection<TypeField> fieldsForApiVersion(int apiVersion) {
        return fieldsByApiVersion.computeIfAbsent(apiVersion, this::findFieldsForApiVersion);
    }

    private Collection<TypeField> findFieldsForApiVersion(int apiVersion) {
        return fields.stream()
            .filter(it -> it.sinceApiVersion() <= apiVersion)
            .collect(Collectors.toList());
    }
}
