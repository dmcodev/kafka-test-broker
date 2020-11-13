package dev.dmco.test.kafka.io.struct;

import dev.dmco.test.kafka.io.IODecoder;
import dev.dmco.test.kafka.io.IOEncoder;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class SimpleField extends FieldHandle {

    private final FieldType type;

    public SimpleField(Field field, FieldType type) {
        super(field);
        if (!type.compatibleWith(field.getType())) {
            throw new IllegalArgumentException(
                "Field " + field + " (" + type + ") can be mapped only to " +
                "one of the following Java types: " + type.compatibleJavaTypes()
            );
        }
        this.type = type;
    }

    @Override
    public Object decode(ByteBuffer buffer, int apiVersion, IODecoder decoder) {
        return type.decode(buffer);
    }

    @Override
    public int encodedSize(Object struct, int apiVersion, IOEncoder encoder) {
        Object value = valueFrom(struct);
        if (value != null) {
            return type.encodedSize(value);
        } else {
            return type.encodedNullSize();
        }
    }

    @Override
    public void encode(Object struct, int apiVersion, ByteBuffer buffer, IOEncoder encoder) {
        Object value = valueFrom(struct);
        if (value != null) {
            type.encode(value, buffer);
        } else {
            type.encodeNull(buffer);
        }
    }
}
