package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;

import java.nio.ByteBuffer;

public class VarUIntCodec implements ValueTypeCodec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        int result = 0;
        int wordNumber = 0;
        int word;
        do {
            word = buffer.get();
            result |= (word & 0b01111111) << (7 * wordNumber);
            wordNumber++;
        } while ((word & 0b10000000) > 0);
        return result;
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        throw new UnsupportedOperationException();
    }
}
