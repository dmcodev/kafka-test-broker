package dev.dmco.test.kafka.io.codec.primitives;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;

import java.nio.ByteBuffer;

public class Int32Codec implements Codec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        return buffer.getInt();
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        buffer.putInt(value != null ? (int) value : 0);
    }
}
