package dev.dmco.test.kafka.io.codec.integers;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;

import java.nio.ByteBuffer;

public class Int64Codec implements Codec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        return buffer.getLong();
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        buffer.putLong(value != null ? (long) value : 0);
    }
}
