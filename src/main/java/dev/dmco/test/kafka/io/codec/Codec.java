package dev.dmco.test.kafka.io.codec;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public interface Codec {

    Stream<Type> handledTypes();

    Object decode(ByteBuffer buffer, Type targetType, CodecContext context);

    void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context);

    void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context);
}
