package dev.dmco.test.kafka.io.codec.integers;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;

import java.nio.ByteBuffer;

public class VarIntCodec implements Codec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        throw new UnsupportedOperationException();
    }
}
