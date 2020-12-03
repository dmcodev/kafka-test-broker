package dev.dmco.test.kafka.io.codec.strings;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringCodec implements Codec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        int length = buffer.getShort();
        byte[] chars = new byte[length];
        buffer.get(chars);
        return new String(chars, StandardCharsets.UTF_8);
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        String string = (String) value;
        byte[] chars = string.getBytes(StandardCharsets.UTF_8);
        buffer.putShort((short) chars.length)
            .putBytes(chars);
    }
}
