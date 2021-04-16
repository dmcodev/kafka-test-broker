package dev.dmcode.test.kafka.io.codec.registry;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
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
public class Type {

    private final Class<?> raw;
    private final List<Type> typeParameters;

    public static Type of(Class<?> type) {
        return new Type(type, Collections.emptyList());
    }

    public static Type of(Class<?> type, Type... parameters) {
        return new Type(type, Arrays.stream(parameters).collect(Collectors.toList()));
    }

    public static Type of(Field field) {
        return of(field.getGenericType());
    }

    public static Type of(java.lang.reflect.Type type) {
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Class<?> rawType = (Class<?>) parameterizedType.getRawType();
            List<Type> typeParameters = Arrays.stream(parameterizedType.getActualTypeArguments())
                .map(Type::of)
                .collect(Collectors.toList());
            return new Type(rawType, typeParameters);
        } else if (Class.class.equals(type.getClass())) {
            return of((Class<?>) type);
        } else {
            throw new IllegalArgumentException("Could not construct type key from: " + type);
        }
    }

    public int differenceFactor(Type coveredKey) {
        if (!raw.isAssignableFrom(coveredKey.raw)) {
            return Integer.MAX_VALUE;
        }
        int difference = typeDistance(coveredKey.raw, raw);
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
