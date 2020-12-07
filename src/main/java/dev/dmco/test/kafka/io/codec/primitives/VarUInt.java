package dev.dmco.test.kafka.io.codec.primitives;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;

import java.nio.ByteBuffer;

public class VarUInt {

    private static final int LOW_WORD_MASK = 0b01111111;
    private static final int NEXT_BYTE_PRESENT_MASK = 0b10000000;
    private static final int SHIFT_SIZE = 7;

    public static int decode(ByteBuffer buffer) {
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

    public static void encode(int value, ResponseBuffer buffer) {
        while (true) {
            int word = value & LOW_WORD_MASK;
            value = value >>> SHIFT_SIZE;
            if (value == 0) {
                buffer.putByte((byte) word);
                break;
            }
            buffer.putByte((byte) (word | NEXT_BYTE_PRESENT_MASK));
        }
    }
}
