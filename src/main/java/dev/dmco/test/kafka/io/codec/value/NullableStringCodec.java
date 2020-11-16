package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;

import java.nio.ByteBuffer;
import java.util.Optional;

public class NullableStringCodec implements ValueTypeCodec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        buffer.mark();
        int length = buffer.getShort();
        if (length >= 0) {
            buffer.reset();
            return Optional.ofNullable(decodeString(buffer, context));
        }
        return Optional.empty();
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        Optional<String> optionalString = (Optional<String>) value;
        if (optionalString != null && optionalString.isPresent()) {
            encodeString(optionalString.get(), buffer, context);
        } else {
            buffer.putShort((short) -1);
        }
    }
}
