package dev.dmco.test.kafka.io.codec.strings;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;

import static dev.dmco.test.kafka.io.protocol.Protocol.decodeCompactString;
import static dev.dmco.test.kafka.io.protocol.Protocol.decodeVarUInt;
import static dev.dmco.test.kafka.io.protocol.Protocol.encodeCompactString;
import static dev.dmco.test.kafka.io.protocol.Protocol.encodeVarUInt;

// TODO: compaction
public class NullableCompactStringCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.empty();
    }

    @Override
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        buffer.mark();
        int length = decodeVarUInt(buffer);
        if (length > 0) {
            buffer.reset();
            return Optional.of(decodeCompactString(buffer));
        }
        return Optional.empty();
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        Optional<String> string = (Optional<String>) value;
        if (string.isPresent()) {
            encodeCompactString(string.get(), buffer);
        } else {
            encodeVarUInt(0, buffer);
        }
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(Optional.empty(), valueType, buffer, context);
    }
}
