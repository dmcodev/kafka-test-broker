package dev.dmco.test.kafka.io;

import dev.dmco.test.kafka.messages.meta.Primitive;
import dev.dmco.test.kafka.messages.meta.PrimitiveType;
import dev.dmco.test.kafka.messages.meta.Sequence;
import dev.dmco.test.kafka.messages.meta.SinceApiVersion;
import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public abstract class TypeField {

    private final static Map<Class<?>, PrimitiveType> PRIMITIVE_TYPE_MAPPING = new HashMap<>();

    static {
        PRIMITIVE_TYPE_MAPPING.put(byte.class, PrimitiveType.INT8);
        PRIMITIVE_TYPE_MAPPING.put(short.class, PrimitiveType.INT16);
        PRIMITIVE_TYPE_MAPPING.put(int.class, PrimitiveType.INT32);
    }

    public abstract int sinceApiVersion();

    public abstract Object decode(ByteBuffer buffer, int apiVersion, IODecoder decoder);

    public abstract int calculateSize(Object instance, int apiVersion, IOEncoder encoder);

    public abstract void encode(Object instance, int apiVersion, ByteBuffer buffer, IOEncoder encoder);

    static TypeField from(Field field) {
        field.setAccessible(true);
        int sinceApiVersion = Optional.ofNullable(field.getAnnotation(SinceApiVersion.class))
            .map(SinceApiVersion::value)
            .orElse(0);
        Function<Object, Object> getter = instance -> readField(field, instance);
        if (field.isAnnotationPresent(Primitive.class)) {
            return new PrimitiveField(field.getAnnotation(Primitive.class).value(), sinceApiVersion, getter);
        }
        if (PRIMITIVE_TYPE_MAPPING.containsKey(field.getType())) {
            return new PrimitiveField(PRIMITIVE_TYPE_MAPPING.get(field.getType()), sinceApiVersion, getter);
        }
        if (Collection.class.isAssignableFrom(field.getType()) && field.isAnnotationPresent(Sequence.class)) {
            return new SequenceField(field.getAnnotation(Sequence.class).value(), sinceApiVersion, getter);
        }
        return new StructField(field.getType(), sinceApiVersion, getter);
    }

    @SneakyThrows
    private static Object readField(Field field, Object object) {
        return field.get(object);
    }
}
