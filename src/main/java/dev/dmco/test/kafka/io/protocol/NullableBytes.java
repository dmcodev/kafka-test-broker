package dev.dmco.test.kafka.io.protocol;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;

import java.nio.ByteBuffer;
import java.util.Optional;

import static dev.dmco.test.kafka.io.protocol.Protocol.decodeBytes;
import static dev.dmco.test.kafka.io.protocol.Protocol.encodeBytes;

public class NullableBytes {

    public static Optional<byte[]> decode(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length > -1) {
            buffer.position(buffer.position() - 4);
            return Optional.of(decodeBytes(buffer));
        }
        return Optional.empty();
    }

    public static void encode(Optional<byte[]> value, ResponseBuffer buffer) {
        if (value.isPresent()) {
            encodeBytes(value.get(), buffer);
        } else {
            buffer.putInt(-1);
        }
    }
}
