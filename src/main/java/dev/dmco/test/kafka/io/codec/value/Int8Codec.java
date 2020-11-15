package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;

import java.nio.ByteBuffer;

public class Int8Codec implements ValueTypeCodec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        return buffer.get();
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        buffer.putByte(value != null ? (byte) value : 0);
    }
}