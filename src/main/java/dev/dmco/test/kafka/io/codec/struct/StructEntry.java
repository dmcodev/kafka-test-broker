package dev.dmco.test.kafka.io.codec.struct;

import dev.dmco.test.kafka.io.codec.struct.fields.StructField;
import dev.dmco.test.kafka.io.codec.struct.fields.StructSequenceField;
import dev.dmco.test.kafka.io.codec.struct.fields.ValueField;
import dev.dmco.test.kafka.io.codec.struct.fields.ValueSequenceField;
import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.io.codec.value.ValueTypeCodec;
import dev.dmco.test.kafka.messages.common.Tag;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import dev.dmco.test.kafka.messages.meta.Value;
import dev.dmco.test.kafka.messages.meta.ValueSequence;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static dev.dmco.test.kafka.io.codec.struct.StructEntry.TypeKeyMapping.mapping;

public abstract class StructEntry implements ValueTypeCodec {

    private static final Collection<TypeKeyMapping> AUTO_VALUE_TYPE_MAPPING = Arrays.asList(
        mapping(ValueType.REQUEST_HEADER, RequestHeader.class),
        mapping(ValueType.RESPOSNE_HEADER, ResponseHeader.class),
        mapping(ValueType.INT8, byte.class),
        mapping(ValueType.INT8, Byte.class),
        mapping(ValueType.INT16, short.class),
        mapping(ValueType.INT16, Short.class),
        mapping(ValueType.INT32, int.class),
        mapping(ValueType.INT32, Integer.class),
        mapping(ValueType.INT64, long.class),
        mapping(ValueType.INT64, Long.class),
        mapping(ValueType.BOOLEAN, boolean.class),
        mapping(ValueType.BOOLEAN, Boolean.class),
        mapping(ValueType.STRING, String.class),
        mapping(ValueType.NULLABLE_STRING, Optional.class, String.class),
        mapping(ValueType.TAGS_BUFFER, Collection.class, Tag.class)
    );

    private final Class<?> javaType;
    private final int minApiVersion;
    private final int maxApiVersion;
    private final Function<Object, Object> getter;

    public StructEntry(Field field) {
        field.setAccessible(true);
        javaType = field.getType();
        getter = createGetter(field);
        minApiVersion = determineMinVersion(field);
        maxApiVersion = determineMaxVersion(field);
    }

    public boolean presentInApiVersion(int apiVersion) {
        return apiVersion >= minApiVersion && apiVersion <= maxApiVersion;
    }

    public Object valueFrom(Object struct) {
        return getter.apply(struct);
    }

    public Object emptyValue() {
        if (Object.class.isAssignableFrom(javaType)) {
            return null;
        } else {
            if (byte.class == javaType) {
                return (byte) 0;
            } else if (short.class == javaType) {
                return (short) 0;
            } else if (int.class == javaType) {
                return 0;
            } else if (float.class == javaType) {
                return 0.0f;
            } else if (double.class == javaType) {
                return 0.0;
            } else {
                throw new IllegalArgumentException("Unsupported field type: " + javaType);
            }
        }
    }

    @SneakyThrows
    private Object valueFrom(Field field, Object struct) {
        return field.get(struct);
    }

    private Function<Object, Object> createGetter(Field field) {
        return struct -> valueFrom(field, struct);
    }

    private int determineMinVersion(Field field) {
        return Optional.ofNullable(field.getAnnotation(ApiVersion.class))
            .map(ApiVersion::min)
            .orElse(0);
    }

    private int determineMaxVersion(Field field) {
        return Optional.ofNullable(field.getAnnotation(ApiVersion.class))
            .map(ApiVersion::max)
            .orElse(Integer.MAX_VALUE);
    }

    public static StructEntry from(Field field) {
        if (field.isAnnotationPresent(Value.class)) {
            ValueType type = field.getAnnotation(Value.class).value();
            return new ValueField(field, type);
        }
        if (field.isAnnotationPresent(StructSequence.class)) {
            Class<?> elementType = field.getAnnotation(StructSequence.class).value();
            return new StructSequenceField(field, elementType);
        }
        if (field.isAnnotationPresent(ValueSequence.class)){
            ValueType elementType = field.getAnnotation(ValueSequence.class).value();
            return new ValueSequenceField(field, elementType);
        }
        TypeKey typeKey = TypeKey.from(field);
        ValueType autoType = findAutoType(typeKey);
        if (autoType != null) {
            return new ValueField(field, autoType);
        }
        if (Collection.class.isAssignableFrom(field.getType())) {
            ParameterizedType fieldParameterizedType = (ParameterizedType) field.getGenericType();
            Type elementGenericType = fieldParameterizedType.getActualTypeArguments()[0];
            TypeKey elementTypeKey = TypeKey.from(elementGenericType);
            ValueType elementAutoType = findAutoType(elementTypeKey);
            if (elementAutoType != null) {
                return new ValueSequenceField(field, elementAutoType);
            }
            return new StructSequenceField(field, elementTypeKey.rawType());
        }
        return new StructField(field, typeKey.rawType());
    }

    private static ValueType findAutoType(TypeKey typeKey) {
        return AUTO_VALUE_TYPE_MAPPING.stream()
            .filter(mapping -> mapping.matches(typeKey))
            .findFirst()
            .map(TypeKeyMapping::getType)
            .orElse(null);
    }

    @lombok.Value
    static class TypeKeyMapping {

        TypeKey key;
        ValueType type;

        static TypeKeyMapping mapping(ValueType type, Class<?>... keyTypes) {
            TypeKey key = new TypeKey(Arrays.asList(keyTypes));
            return new TypeKeyMapping(key, type);
        }

        boolean matches(TypeKey candidateKey) {
            return key.isSuperKeyOf(candidateKey);
        }
    }

    @lombok.Value
    static class TypeKey {

        List<Class<?>> types;

        Class<?> rawType() {
            return position(0);
        }

        private int size() {
            return types.size();
        }

        private Class<?> position(int i) {
            return types.get(i);
        }

        static TypeKey from(Field field) {
            return from(field.getGenericType());
        }

        static TypeKey from(Type type) {
            List<Class<?>> types = new ArrayList<>();
            if (type instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType) type;
                types.add((Class<?>) parameterizedType.getRawType());
                Arrays.stream(parameterizedType.getActualTypeArguments())
                    .filter(Class.class::isInstance)
                    .map(Class.class::cast)
                    .forEach(types::add);
            } else if (Class.class.equals(type.getClass())) {
                types.add((Class<?>) type);
            } else {
                throw new IllegalArgumentException("Could not construct type key from: " + type);
            }
            return new TypeKey(types);
        }

        public boolean isSuperKeyOf(TypeKey candidateKey) {
            if (size() != candidateKey.size()) {
                return false;
            }
            if (!position(0).isAssignableFrom(candidateKey.position(0))) {
                return false;
            }
            for (int i = 1; i < types.size(); i++) {
                if (!position(i).equals(candidateKey.position(i))) {
                    return false;
                }
            }
            return true;
        }
    }
}
