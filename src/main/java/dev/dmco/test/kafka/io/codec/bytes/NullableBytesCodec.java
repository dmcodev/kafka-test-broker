package dev.dmco.test.kafka.io.codec.bytes;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;

public class NullableBytesCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(Optional.class, Type.of(byte[].class)));
    }

    @Override
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        buffer.mark();
        int length = buffer.getInt();
        if (length > -1) {
            buffer.reset();
            return BytesCodec.decode(buffer);
        }
        return null;
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        if (value != null) {
            BytesCodec.encode(value, buffer);
        } else {
            buffer.putInt(-1);
        }
    }
}