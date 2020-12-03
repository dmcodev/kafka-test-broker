package dev.dmco.test.kafka.io.codec.integers;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;

import java.nio.ByteBuffer;

public class VarUIntCodec implements Codec {

    private static final int LOW_WORD_MASK = 0b01111111;
    private static final int NEXT_BYTE_PRESENT_MASK = 0b10000000;
    private static final int SHIFT_SIZE = 7;

    @Override
    public Integer decode(ByteBuffer buffer, CodecContext context) {
        int result = 0;
        int shift = 0;
        int word;
        do {
            word = buffer.get();
            result |= (word & LOW_WORD_MASK) << shift;
            shift += SHIFT_SIZE;
        } while ((word & NEXT_BYTE_PRESENT_MASK) > 0);
        return result;
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        int intValue = (int) value;
        while (true) {
            int word = intValue & LOW_WORD_MASK;
            intValue = intValue >>> SHIFT_SIZE;
            if (intValue == 0) {
                buffer.putByte((byte) word);
                break;
            }
            buffer.putByte((byte) (word | NEXT_BYTE_PRESENT_MASK));
        }
    }
}
