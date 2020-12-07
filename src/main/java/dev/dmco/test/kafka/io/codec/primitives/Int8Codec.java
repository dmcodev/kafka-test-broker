package dev.dmco.test.kafka.io.codec.primitives;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;

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
        buffer.putByte(value != null ? (byte) value : 0);
    }
}
