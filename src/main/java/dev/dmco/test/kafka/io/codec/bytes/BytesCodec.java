package dev.dmco.test.kafka.io.codec.bytes;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static dev.dmco.test.kafka.io.protocol.Protocol.decodeBytes;
import static dev.dmco.test.kafka.io.protocol.Protocol.encodeBytes;

public class BytesCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(byte[].class));
    }

    @Override
    public byte[] decode(ByteBuffer buffer, Type targetType, CodecContext context) {
       return decodeBytes(buffer);
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        encodeBytes((byte[]) value, buffer);
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encodeBytes(new byte[0], buffer);
    }
}
