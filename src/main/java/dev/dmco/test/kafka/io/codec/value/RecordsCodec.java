package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import dev.dmco.test.kafka.messages.common.Records;

import java.nio.ByteBuffer;

public class RecordsCodec extends ValueTypeCodec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        return Records.builder().build();
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        throw new UnsupportedOperationException();
    }
}
