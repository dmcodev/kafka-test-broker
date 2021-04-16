package dev.dmcode.test.kafka.io.codec.strings;

import dev.dmcode.test.kafka.io.buffer.ResponseBuffer;
import dev.dmcode.test.kafka.io.codec.Codec;
import dev.dmcode.test.kafka.io.codec.context.CodecContext;
import dev.dmcode.test.kafka.io.codec.registry.Type;
import dev.dmcode.test.kafka.io.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;

// TODO: compaction
public class NullableCompactStringCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.empty();
    }

    @Override
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        buffer.mark();
        int length = Protocol.decodeVarUInt(buffer);
        if (length > 0) {
            buffer.reset();
            return Optional.of(Protocol.decodeCompactString(buffer));
        }
        return Optional.empty();
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        Optional<String> string = (Optional<String>) value;
        if (string.isPresent()) {
            Protocol.encodeCompactString(string.get(), buffer);
        } else {
            Protocol.encodeVarUInt(0, buffer);
        }
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(Optional.empty(), valueType, buffer, context);
    }
}
