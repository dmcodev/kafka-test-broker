package dev.dmco.test.kafka.io.struct;

import dev.dmco.test.kafka.io.IODecoder;
import dev.dmco.test.kafka.io.IOEncoder;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class SimpleSequenceField extends SequenceField {

    private final FieldType elementType;

    public SimpleSequenceField(Field field, FieldType elementType) {
        super(field);
        this.elementType = elementType;
    }

    @Override
    protected Object decodeElement(ByteBuffer buffer, int apiVersion, IODecoder decoder) {
        return elementType.decode(buffer);
    }

    @Override
    protected int encodedElementSize(Object element, int apiVersion, IOEncoder encoder) {
        if (element != null) {
            return elementType.encodedSize(element);
        } else {
            return elementType.encodedNullSize();
        }
    }

    @Override
    protected void encodeElement(Object element, int apiVersion, ByteBuffer buffer, IOEncoder encoder) {
        if (element != null) {
            elementType.encode(element, buffer);
        } else {
            elementType.encodeNull(buffer);
        }
    }
}
