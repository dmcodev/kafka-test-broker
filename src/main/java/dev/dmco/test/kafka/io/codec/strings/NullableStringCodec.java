package dev.dmco.test.kafka.io.codec.strings;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;

import java.nio.ByteBuffer;
import java.util.Optional;

import static dev.dmco.test.kafka.io.codec.registry.CodecRegistry.STRING;

public class NullableStringCodec implements Codec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        buffer.mark();
        int length = buffer.getShort();
        if (length >= 0) {
            buffer.reset();
            return Optional.ofNullable(STRING.decode(buffer, context));
        }
        return Optional.empty();
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        Optional<String> optionalString = (Optional<String>) value;
        if (optionalString != null && optionalString.isPresent()) {
            STRING.encode(optionalString.get(), buffer, context);
        } else {
            buffer.putShort((short) -1);
        }
    }
}
