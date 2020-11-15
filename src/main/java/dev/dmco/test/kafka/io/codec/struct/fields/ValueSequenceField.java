package dev.dmco.test.kafka.io.codec.struct.fields;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import dev.dmco.test.kafka.io.codec.value.ValueType;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class ValueSequenceField extends SequenceField {

    private final ValueType elementType;

    public ValueSequenceField(Field field, ValueType elementType) {
        super(field);
        this.elementType = elementType;
    }

    @Override
    protected Object decodeElement(ByteBuffer buffer, CodecContext context) {
        return elementType.codec().decode(buffer, context);
    }

    @Override
    protected void encodeElement(Object element, ResponseBuffer buffer, CodecContext context) {
        elementType.codec()
            .encode(element, buffer, context);
    }
}
