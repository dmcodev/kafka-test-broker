package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;

import java.nio.ByteBuffer;

public class Int16Codec extends ValueTypeCodec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        return buffer.getShort();
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        buffer.putShort(value != null ? (short) value : 0);
    }
}
