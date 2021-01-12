package dev.dmco.test.kafka.io.codec.strings;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;

import static dev.dmco.test.kafka.io.protocol.Protocol.decodeString;
import static dev.dmco.test.kafka.io.protocol.Protocol.encodeString;

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
            return Optional.of(decodeString(buffer));
        }
        return Optional.empty();
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        Optional<String> string = (Optional<String>) value;
        if (string.isPresent()) {
            encodeString(string.get(), buffer);
        } else {
            buffer.putShort((short) -1);
        }
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(Optional.empty(), valueType, buffer, context);
    }
}
