package dev.dmco.test.kafka.io.codec.struct;

import dev.dmco.test.kafka.io.codec.struct.fields.StructField;
import dev.dmco.test.kafka.io.codec.struct.fields.StructSequenceField;
import dev.dmco.test.kafka.io.codec.struct.fields.ValueField;
import dev.dmco.test.kafka.io.codec.struct.fields.ValueSequenceField;
import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.io.codec.value.ValueTypeCodec;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import dev.dmco.test.kafka.messages.meta.Value;
import dev.dmco.test.kafka.messages.meta.ValueSequence;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public abstract class StructEntry implements ValueTypeCodec {

    private static final Map<Class<?>, ValueType> AUTO_VALUE_TYPE_MAPPING = new HashMap<>();

    static {
        AUTO_VALUE_TYPE_MAPPING.put(RequestHeader.class, ValueType.REQUEST_HEADER);
        AUTO_VALUE_TYPE_MAPPING.put(ResponseHeader.class, ValueType.RESPOSNE_HEADER);
        AUTO_VALUE_TYPE_MAPPING.put(byte.class, ValueType.INT8);
        AUTO_VALUE_TYPE_MAPPING.put(short.class, ValueType.INT16);
        AUTO_VALUE_TYPE_MAPPING.put(int.class, ValueType.INT32);
        AUTO_VALUE_TYPE_MAPPING.put(long.class, ValueType.INT64);
        AUTO_VALUE_TYPE_MAPPING.put(boolean.class, ValueType.BOOLEAN);
        AUTO_VALUE_TYPE_MAPPING.put(Byte.class, ValueType.INT8);
        AUTO_VALUE_TYPE_MAPPING.put(Short.class, ValueType.INT16);
        AUTO_VALUE_TYPE_MAPPING.put(Integer.class, ValueType.INT32);
        AUTO_VALUE_TYPE_MAPPING.put(Long.class, ValueType.INT64);
        AUTO_VALUE_TYPE_MAPPING.put(Boolean.class, ValueType.BOOLEAN);
        AUTO_VALUE_TYPE_MAPPING.put(String.class, ValueType.NULLABLE_STRING);
    }

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
        if (Collection.class.isAssignableFrom(field.getType())) {
            ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
            Class<?> elementType = (Class<?>) parameterizedType.getActualTypeArguments()[0];
            if (AUTO_VALUE_TYPE_MAPPING.containsKey(elementType)) {
                return new ValueSequenceField(field, AUTO_VALUE_TYPE_MAPPING.get(elementType));
            }
            return new StructSequenceField(field, elementType);
        }
        if (AUTO_VALUE_TYPE_MAPPING.containsKey(field.getType())) {
            ValueType type = AUTO_VALUE_TYPE_MAPPING.get(field.getType());
            return new ValueField(field, type);
        }
        return new StructField(field, field.getType());
    }
}
