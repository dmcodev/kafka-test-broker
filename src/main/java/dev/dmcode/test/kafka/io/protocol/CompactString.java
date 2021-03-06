package dev.dmcode.test.kafka.io.protocol;

import dev.dmcode.test.kafka.io.buffer.ResponseBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static dev.dmcode.test.kafka.io.protocol.Protocol.decodeVarUInt;
import static dev.dmcode.test.kafka.io.protocol.Protocol.encodeVarUInt;

class CompactString {

    static java.lang.String decode(ByteBuffer buffer) {
        int length = decodeVarUInt(buffer) - 1;
        byte[] chars = new byte[length];
        buffer.get(chars);
        return new java.lang.String(chars, StandardCharsets.UTF_8);
    }

    static void encode(java.lang.String value, ResponseBuffer buffer) {
        byte[] chars = value.getBytes(StandardCharsets.UTF_8);
        encodeVarUInt(chars.length + 1, buffer);
        buffer.putBytes(chars);
    }
}
