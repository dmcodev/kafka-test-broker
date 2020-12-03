package dev.dmco.test.kafka.io.codec.registry;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ToString
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@RequiredArgsConstructor
public class TypeKey {

    public static TypeKey STRING = key(String.class);

    private final Class<?> rawType;
    private final List<TypeKey> typeParameters;

    public static TypeKey key(Class<?> type) {
        return new TypeKey(type, Collections.emptyList());
    }

    public static TypeKey key(Class<?> type, TypeKey... parameters) {
        return new TypeKey(type, Arrays.stream(parameters).collect(Collectors.toList()));
    }

    public static TypeKey key(Field field) {
        return key(field.getGenericType());
    }

    public static TypeKey key(Type type) {
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Class<?> rawType = (Class<?>) parameterizedType.getRawType();
            List<TypeKey> typeParameters = Arrays.stream(parameterizedType.getActualTypeArguments())
                .map(TypeKey::key)
                .collect(Collectors.toList());
            return new TypeKey(rawType, typeParameters);
        } else if (Class.class.equals(type.getClass())) {
            return key((Class<?>) type);
        } else {
            throw new IllegalArgumentException("Could not construct type key from: " + type);
        }
    }

    public int differenceFactor(TypeKey coveredKey) {
        if (!rawType.isAssignableFrom(coveredKey.rawType)) {
            return Integer.MAX_VALUE;
        }
        int difference = typeDistance(coveredKey.rawType, rawType);
        if (typeParameters.size() != coveredKey.typeParameters.size()) {
            return Integer.MAX_VALUE;
        }
        for (int i = 0; i < typeParameters.size(); i++) {
            int parameterDifference = typeParameters.get(i)
                .differenceFactor(coveredKey.typeParameters.get(i));
            if (parameterDifference == Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            difference += parameterDifference;
        }
        return difference;
    }

    private int typeDistance(Class<?> start, Class<?> end) {
        if (start.equals(end)) {
            return 0;
        }
        int distance =
            Stream.concat(
                Stream.of(start.getSuperclass()),
                Arrays.stream(start.getInterfaces())
            )
            .filter(Objects::nonNull)
            .mapToInt(parent -> typeDistance(parent, end))
            .min()
            .orElse(Integer.MAX_VALUE);
        return (distance != Integer.MAX_VALUE)
            ? 1 + distance
            : Integer.MAX_VALUE;
    }
}
