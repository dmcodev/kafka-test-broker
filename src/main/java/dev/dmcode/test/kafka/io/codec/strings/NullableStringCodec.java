package dev.dmcode.test.kafka.io.codec.strings;

import dev.dmcode.test.kafka.io.buffer.ResponseBuffer;
import dev.dmcode.test.kafka.io.codec.Codec;
import dev.dmcode.test.kafka.io.codec.context.CodecContext;
import dev.dmcode.test.kafka.io.codec.registry.Type;
import dev.dmcode.test.kafka.io.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;

public class NullableStringCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(Optional.class, Type.of(String.class)));
    }

    @Override
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        buffer.mark();
        int length = buffer.getShort();
        if (length >= 0) {
            buffer.reset();
            return Optional.of(Protocol.decodeString(buffer));
        }
        return Optional.empty();
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        Optional<String> string = (Optional<String>) value;
        if (string.isPresent()) {
            Protocol.encodeString(string.get(), buffer);
        } else {
            buffer.putShort((short) -1);
        }
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(Optional.empty(), valueType, buffer, context);
    }
}
