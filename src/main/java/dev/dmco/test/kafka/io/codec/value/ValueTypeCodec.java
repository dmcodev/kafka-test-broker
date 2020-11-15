package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;

import java.nio.ByteBuffer;

public abstract class ValueTypeCodec {

    public abstract Object decode(ByteBuffer buffer, CodecContext context);

    public abstract void encode(Object value, ResponseBuffer buffer, CodecContext context);
}
