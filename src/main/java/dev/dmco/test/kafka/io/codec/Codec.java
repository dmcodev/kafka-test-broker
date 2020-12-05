package dev.dmco.test.kafka.io.codec;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.TypeKey;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public interface Codec {

    Object decode(ByteBuffer buffer, CodecContext context);

    void encode(Object value, ResponseBuffer buffer, CodecContext context);

    default Stream<TypeKey> handledTypes() {
        return Stream.empty();
    }
}
