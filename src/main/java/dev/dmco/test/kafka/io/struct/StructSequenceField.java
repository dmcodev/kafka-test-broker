package dev.dmco.test.kafka.io.struct;

import dev.dmco.test.kafka.io.IODecoder;
import dev.dmco.test.kafka.io.IOEncoder;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class StructSequenceField extends SequenceField {

    private final Class<?> elementType;

    public StructSequenceField(Field field, Class<?> elementType) {
        super(field);
        this.elementType = elementType;
    }

    @Override
    protected Object decodeElement(ByteBuffer buffer, int apiVersion, IODecoder decoder) {
        return decoder.decode(buffer, apiVersion, elementType);
    }

    @Override
    protected int encodedElementSize(Object element, int apiVersion, IOEncoder encoder) {
        return encoder.encodedSize(element, apiVersion);
    }

    @Override
    protected void encodeElement(Object element, int apiVersion, ByteBuffer buffer, IOEncoder encoder) {
        encoder.encode(element, apiVersion, buffer);
    }
}
