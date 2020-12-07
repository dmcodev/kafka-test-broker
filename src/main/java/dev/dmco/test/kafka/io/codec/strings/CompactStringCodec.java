package dev.dmco.test.kafka.io.codec.strings;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.primitives.VarUInt;
import dev.dmco.test.kafka.io.codec.registry.Type;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

// TODO: compaction
public class CompactStringCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.empty();
    }

    @Override
    public String decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        return decode(buffer);
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode((String) value, buffer);
    }

    public static String decode(ByteBuffer buffer) {
        int length = VarUInt.decode(buffer) - 1;
        byte[] chars = new byte[length];
        buffer.get(chars);
        return new String(chars, StandardCharsets.UTF_8);
    }

    public static void encode(String value, ResponseBuffer buffer) {
        byte[] chars = value.getBytes(StandardCharsets.UTF_8);
        VarUInt.encode(chars.length + 1, buffer);
        buffer.putBytes(chars);
    }
}
