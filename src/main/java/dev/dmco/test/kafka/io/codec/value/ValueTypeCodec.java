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

    default String decodeString(ByteBuffer buffer, CodecContext context) {
        return (String) ValueType.STRING.decode(buffer, context);
    }

    default String decodeCompactString(ByteBuffer buffer, CodecContext context) {
        return (String) ValueType.COMPACT_STRING.decode(buffer, context);
    }

    default void encodeUVarInt(int value, ResponseBuffer buffer, CodecContext context) {
        ValueType.UVARINT.encode(value, buffer, context);
    }

    default void encodeString(String value, ResponseBuffer buffer, CodecContext context) {
        ValueType.STRING.encode(value, buffer, context);
    }

    default void encodeCompactString(String value, ResponseBuffer buffer, CodecContext context) {
        ValueType.COMPACT_STRING.encode(value, buffer, context);
    }
}
