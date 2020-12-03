package dev.dmco.test.kafka.io.codec.strings;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dev.dmco.test.kafka.io.codec.registry.CodecRegistry.UVAR_INT;

public class CompactStringCodec implements Codec {

    @Override
    public String decode(ByteBuffer buffer, CodecContext context) {
        int length = UVAR_INT.decode(buffer, context) - 1;
        byte[] chars = new byte[length];
        buffer.get(chars);
        return new String(chars, StandardCharsets.UTF_8);
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        String string = (String) value;
        byte[] chars = string.getBytes(StandardCharsets.UTF_8);
        UVAR_INT.encode(chars.length + 1, buffer, context);
        buffer.putBytes(chars);
    }
}
