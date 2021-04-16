package dev.dmcode.test.kafka.io.codec.strings;

import dev.dmcode.test.kafka.io.buffer.ResponseBuffer;
import dev.dmcode.test.kafka.io.codec.Codec;
import dev.dmcode.test.kafka.io.codec.context.CodecContext;
import dev.dmcode.test.kafka.io.codec.registry.Type;
import dev.dmcode.test.kafka.io.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

// TODO: compaction
public class CompactStringCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.empty();
    }

    @Override
    public String decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        return Protocol.decodeCompactString(buffer);
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        Protocol.encodeCompactString((String) value, buffer);
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode("", valueType, buffer, context);
    }
}
