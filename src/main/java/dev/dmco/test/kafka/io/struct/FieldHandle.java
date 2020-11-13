package dev.dmco.test.kafka.io.struct;

import dev.dmco.test.kafka.io.IODecoder;
import dev.dmco.test.kafka.io.IOEncoder;
import dev.dmco.test.kafka.messages.meta.Field;
import dev.dmco.test.kafka.messages.meta.FieldSequence;
import dev.dmco.test.kafka.messages.meta.SinceVersion;
import dev.dmco.test.kafka.messages.meta.Struct;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;

import java.nio.ByteBuffer;
import java.util.Optional;

@Accessors(fluent = true)
public abstract class FieldHandle {

    private final java.lang.reflect.Field reflectionField;
    @Getter private final int sinceVersion;

    public FieldHandle(java.lang.reflect.Field field) {
        field.setAccessible(true);
        reflectionField = field;
        sinceVersion = Optional.ofNullable(field.getAnnotation(SinceVersion.class))
            .map(SinceVersion::value)
            .orElse(0);
    }

    @SneakyThrows
    protected Object valueFrom(Object struct) {
        return reflectionField.get(struct);
    }

    public abstract Object decode(ByteBuffer buffer, int apiVersion, IODecoder decoder);

    public abstract int encodedSize(Object struct, int apiVersion, IOEncoder encoder);

    public abstract void encode(Object struct, int apiVersion, ByteBuffer buffer, IOEncoder encoder);

    public static FieldHandle from(java.lang.reflect.Field field) {
        if (field.isAnnotationPresent(Field.class)) {
            FieldType type = field.getAnnotation(Field.class).value();
            return new SimpleField(field, type);
        }
        if (field.isAnnotationPresent(StructSequence.class)) {
            Class<?> elementType = field.getAnnotation(StructSequence.class).value();
            return new StructSequenceField(field, elementType);
        }
        if (field.isAnnotationPresent(FieldSequence.class)){
            FieldType elementType = field.getAnnotation(FieldSequence.class).value();
            return new SimpleSequenceField(field, elementType);
        }
        if (field.isAnnotationPresent(Struct.class)) {
            return new StructField(field, field.getType());
        }
        throw new IllegalArgumentException("Unknown Kafka type for field " + field);
    }
}
