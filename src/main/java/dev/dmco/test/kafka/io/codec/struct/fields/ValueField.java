package dev.dmco.test.kafka.io.codec.struct.fields;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import dev.dmco.test.kafka.io.codec.struct.StructEntry;
import dev.dmco.test.kafka.io.codec.value.ValueType;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class ValueField extends StructEntry {

    private final ValueType type;

    public ValueField(Field field, ValueType type) {
        super(field);
        this.type = type;
    }

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        return type.decode(buffer, context);
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        type.encode(value, buffer, context);
    }
}
