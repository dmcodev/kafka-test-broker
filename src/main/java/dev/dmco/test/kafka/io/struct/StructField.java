package dev.dmco.test.kafka.io.struct;

import dev.dmco.test.kafka.io.IODecoder;
import dev.dmco.test.kafka.io.IOEncoder;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class StructField extends FieldHandle {

    private final Class<?> type;

    public StructField(Field field, Class<?> type) {
        super(field);
        this.type = type;
    }

    @Override
    public Object decode(ByteBuffer buffer, int apiVersion, IODecoder decoder) {
        return decoder.decode(buffer, apiVersion, type);
    }

    @Override
    public int encodedSize(Object struct, int apiVersion, IOEncoder encoder) {
        return encoder.encodedSize(valueFrom(struct), apiVersion);
    }

    @Override
    public void encode(Object struct, int apiVersion, ByteBuffer buffer, IOEncoder encoder) {
        encoder.encode(valueFrom(struct), apiVersion, buffer);
    }
}
