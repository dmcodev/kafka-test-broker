package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CompactStringCodec implements ValueTypeCodec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        int length = decodeUVarInt(buffer, context);
        byte[] chars = new byte[length];
        buffer.get(chars);
        return new String(chars, StandardCharsets.UTF_8);
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        String string = (String) value;
        byte[] chars = string.getBytes(StandardCharsets.UTF_8);
        encodeUVarInt(chars.length, buffer, context);
        buffer.putBytes(chars);
    }
}
