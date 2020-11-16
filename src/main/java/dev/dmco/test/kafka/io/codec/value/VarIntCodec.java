package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;

import java.nio.ByteBuffer;

public class VarIntCodec implements ValueTypeCodec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << i;
            i += 7;
        }
        value |= b << i;
        return value;
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        throw new UnsupportedOperationException();
    }
}
