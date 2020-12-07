package dev.dmco.test.kafka.io.codec.strings;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

public class StringCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(String.class));
    }

    @Override
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        return decode(buffer);
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(value, buffer);
    }

    public static String decode(ByteBuffer buffer) {
        int length = buffer.getShort();
        byte[] chars = new byte[length];
        buffer.get(chars);
        return new String(chars, StandardCharsets.UTF_8);
    }

    public static void encode(Object value, ResponseBuffer buffer) {
        String string = (String) value;
        byte[] chars = string.getBytes(StandardCharsets.UTF_8);
        buffer.putShort((short) chars.length)
            .putBytes(chars);
    }
}
