package dev.dmco.test.kafka.io.codec.bytes;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;

import java.nio.ByteBuffer;

import static dev.dmco.test.kafka.io.codec.registry.CodecRegistry.BYTES;

public class NullableBytesCodec implements Codec {

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
