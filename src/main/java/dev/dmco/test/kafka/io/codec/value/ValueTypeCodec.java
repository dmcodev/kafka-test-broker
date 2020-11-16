package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;

import java.nio.ByteBuffer;

public interface ValueTypeCodec {

    Object decode(ByteBuffer buffer, CodecContext context);

    void encode(Object value, ResponseBuffer buffer, CodecContext context);

    default int decodeUVarInt(ByteBuffer buffer, CodecContext context) {
        return (int) ValueType.UVARINT.decode(buffer, context);
    }
}
