package dev.dmcode.test.kafka.io.codec.primitives;

import dev.dmcode.test.kafka.io.buffer.ResponseBuffer;
import dev.dmcode.test.kafka.io.codec.Codec;
import dev.dmcode.test.kafka.io.codec.context.CodecContext;
import dev.dmcode.test.kafka.io.codec.registry.Type;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class Int8Codec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(byte.class), Type.of(Byte.class));
    }

    @Override
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        return buffer.get();
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        buffer.putByte((byte) value);
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode((byte) 0, valueType, buffer, context);
    }
}
