package dev.dmco.test.kafka.io.codec.strings;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.primitives.VarUInt;
import dev.dmco.test.kafka.io.codec.registry.Type;

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
        int length = VarUInt.decode(buffer);
        if (length > 0) {
            buffer.reset();
            return Optional.of(CompactStringCodec.decode(buffer));
        }
        return Optional.empty();
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        Optional<String> string = (Optional<String>) value;
        if (string != null && string.isPresent()) {
            CompactStringCodec.encode(string.get(), buffer);
        } else {
            VarUInt.encode(0, buffer);
        }
    }
}
