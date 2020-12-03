package dev.dmco.test.kafka.io.codec.primitives;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;

import java.nio.ByteBuffer;

public class BooleanCodec implements Codec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        return buffer.get() != 0;
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        byte encoded = (value != null && (boolean) value) ? (byte) 1 : 0;
        buffer.putByte(encoded);
    }
}
