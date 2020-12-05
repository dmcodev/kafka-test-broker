package dev.dmco.test.kafka.io.codec.bytes;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.TypeKey;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;

import static dev.dmco.test.kafka.io.codec.bytes.BytesCodec.BYTES;
import static dev.dmco.test.kafka.io.codec.registry.TypeKey.key;

public class NullableBytesCodec implements Codec {

    @Override
    public Stream<TypeKey> handledTypes() {
        return Stream.of(
            key(Optional.class, key(byte[].class))
        );
    }

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        buffer.mark();
        int length = buffer.getInt();
        if (length > -1) {
            buffer.reset();
            return BYTES.decode(buffer, context);
        }
        return null;
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        if (value != null) {
            BYTES.encode(value, buffer, context);
        } else {
            buffer.putInt(-1);
        }
    }
}
