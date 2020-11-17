package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;

import java.nio.ByteBuffer;
import java.util.Optional;

public class NullableCompactStringCodec implements ValueTypeCodec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        buffer.mark();
        int length = decodeUVarInt(buffer, context);
        if (length > 0) {
            buffer.reset();
            return Optional.ofNullable(decodeCompactString(buffer, context));
        }
        return Optional.empty();
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        Optional<String> optionalString = (Optional<String>) value;
        if (optionalString != null && optionalString.isPresent()) {
            encodeCompactString(optionalString.get(), buffer, context);
        } else {
            encodeUVarInt(0, buffer, context);
        }
    }
}
