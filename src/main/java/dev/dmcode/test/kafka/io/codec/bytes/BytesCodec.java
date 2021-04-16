package dev.dmcode.test.kafka.io.codec.bytes;

import dev.dmcode.test.kafka.io.buffer.ResponseBuffer;
import dev.dmcode.test.kafka.io.codec.Codec;
import dev.dmcode.test.kafka.io.codec.context.CodecContext;
import dev.dmcode.test.kafka.io.codec.registry.Type;
import dev.dmcode.test.kafka.io.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

public class BytesCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(byte[].class));
    }

    @Override
    public byte[] decode(ByteBuffer buffer, Type targetType, CodecContext context) {
       return Protocol.decodeBytes(buffer);
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        Protocol.encodeBytes((byte[]) value, buffer);
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        Protocol.encodeBytes(new byte[0], buffer);
    }
}
