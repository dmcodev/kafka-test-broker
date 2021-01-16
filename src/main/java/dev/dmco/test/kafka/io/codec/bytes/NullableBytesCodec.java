package dev.dmco.test.kafka.io.codec.bytes;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;

import static dev.dmco.test.kafka.io.protocol.Protocol.decodeNullableBytes;
import static dev.dmco.test.kafka.io.protocol.Protocol.encodeNullableBytes;

public class NullableBytesCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(Optional.class, Type.of(byte[].class)));
    }

    @Override
    public Optional<byte[]> decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        return decodeNullableBytes(buffer);
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        encodeNullableBytes((Optional<byte[]>) value, buffer);
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encodeNullableBytes(Optional.empty(), buffer);
    }
}
