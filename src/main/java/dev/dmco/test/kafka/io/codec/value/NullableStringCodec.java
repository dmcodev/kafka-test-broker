package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;

import java.nio.ByteBuffer;

public class NullableStringCodec extends ValueTypeCodec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        buffer.mark();
        int length = buffer.getShort();
        if (length >= 0) {
            buffer.reset();
            return ValueType.STRING.codec()
                .decode(buffer, context);
        }
        return null;
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        if (value != null) {
            ValueType.STRING.codec()
                .encode(value, buffer, context);
        } else {
            buffer.putShort((short) -1);
        }
    }
}
