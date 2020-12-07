package dev.dmco.test.kafka.io.codec.bytes;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class BytesCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(byte[].class));
    }

    @Override
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
       return decode(buffer);
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(value, buffer);
    }

    public static Object decode(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    public static void encode(Object value, ResponseBuffer buffer) {
        byte[] bytes = (byte[]) value;
        buffer.putInt(bytes.length);
        buffer.putBytes(bytes);
    }
}
