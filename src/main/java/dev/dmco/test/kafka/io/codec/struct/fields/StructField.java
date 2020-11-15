package dev.dmco.test.kafka.io.codec.struct.fields;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import dev.dmco.test.kafka.io.codec.struct.StructEntry;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class StructField extends StructEntry {

    private final Class<?> type;

    public StructField(Field field, Class<?> type) {
        super(field);
        this.type = type;
    }

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        return context.structCodec()
            .decode(type, buffer, context);
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        context.structCodec()
            .encode(value, buffer, context);
    }
}
