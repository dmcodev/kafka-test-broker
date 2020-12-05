package dev.dmco.test.kafka.io.codec.primitives;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.TypeKey;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static dev.dmco.test.kafka.io.codec.registry.TypeKey.key;

public class Int32Codec implements Codec {

    @Override
    public Stream<TypeKey> handledTypes() {
        return Stream.of(
            key(int.class),
            key(Integer.class)
        );
    }

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        return buffer.getInt();
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        buffer.putInt(value != null ? (int) value : 0);
    }
}
