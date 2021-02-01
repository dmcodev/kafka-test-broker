package dev.dmco.test.kafka.io.protocol;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;

import java.nio.ByteBuffer;

class VarULong {

    private static final long LOW_WORD_MASK = 0b01111111L;
    private static final int NEXT_BYTE_PRESENT_MASK = 0b10000000;
    private static final int SHIFT_SIZE = 7;

    private static final long SIZE_BYTES_1 = Long.MAX_VALUE >>> 56;
    private static final long SIZE_BYTES_2 = Long.MAX_VALUE >>> 49;
    private static final long SIZE_BYTES_3 = Long.MAX_VALUE >>> 42;
    private static final long SIZE_BYTES_4 = Long.MAX_VALUE >>> 35;
    private static final long SIZE_BYTES_5 = Long.MAX_VALUE >>> 28;
    private static final long SIZE_BYTES_6 = Long.MAX_VALUE >>> 21;
    private static final long SIZE_BYTES_7 = Long.MAX_VALUE >>> 14;
    private static final long SIZE_BYTES_8 = Long.MAX_VALUE >>> 7;

    static long decode(ByteBuffer buffer) {
        long result = 0;
        int shift = 0;
        int word;
        do {
            word = buffer.get();
            result |= (word & LOW_WORD_MASK) << shift;
            shift += SHIFT_SIZE;
        } while ((word & NEXT_BYTE_PRESENT_MASK) > 0);
        return result;
    }

    static void encode(long value, ResponseBuffer buffer) {
        while (true) {
            int word = (int) (value & LOW_WORD_MASK);
            value = value >>> SHIFT_SIZE;
            if (value == 0) {
                buffer.putByte((byte) word);
                break;
            }
            buffer.putByte((byte) (word | NEXT_BYTE_PRESENT_MASK));
        }
    }

    static int sizeOf(long value) {
        if (value < 0) {
            return 10;
        } else if (value <= SIZE_BYTES_1) {
            return 1;
        } else if (value <= SIZE_BYTES_2) {
            return 2;
        } else if (value <= SIZE_BYTES_3) {
            return 3;
        } else if (value <= SIZE_BYTES_4) {
            return 4;
        } else if (value <= SIZE_BYTES_5) {
            return 5;
        } else if (value <= SIZE_BYTES_6) {
            return 6;
        } else if (value <= SIZE_BYTES_7) {
            return 7;
        } else if (value <= SIZE_BYTES_8) {
            return 8;
        }
        return 9;
    }
}
