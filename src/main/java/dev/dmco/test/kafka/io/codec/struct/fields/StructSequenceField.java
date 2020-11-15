package dev.dmco.test.kafka.io.codec.struct.fields;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class StructSequenceField extends SequenceField {

    private final Class<?> elementType;

    public StructSequenceField(Field field, Class<?> elementType) {
        super(field);
        this.elementType = elementType;
    }

    @Override
    protected Object decodeElement(ByteBuffer buffer, CodecContext context) {
        return context.structCodec()
            .decode(elementType, buffer, context);
    }

    @Override
    protected void encodeElement(Object element, ResponseBuffer buffer, CodecContext context) {
        context.structCodec()
            .encode(element, buffer, context);
    }
}
