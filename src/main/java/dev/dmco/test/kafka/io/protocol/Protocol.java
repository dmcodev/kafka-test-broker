package dev.dmco.test.kafka.io.protocol;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;

import java.nio.ByteBuffer;
import java.util.Optional;

public class Protocol {

    public static int decodeVarUInt(ByteBuffer buffer) {
        return (int) VarULong.decode(buffer);
    }

    public static void encodeVarUInt(int value, ResponseBuffer buffer) {
        VarULong.encode(value, buffer);
    }

    public static int decodeVarInt(ByteBuffer buffer) {
        return (int) VarLong.decode(buffer);
    }

    public static void encodeVarInt(int value, ResponseBuffer buffer) {
        VarLong.encode(value, buffer);
    }

    public static int sizeOfVarInt(int value) {
        return VarLong.sizeOf(value);
    }

    public static void encodeVarLong(long value, ResponseBuffer buffer) {
        VarLong.encode(value, buffer);
    }

    public static int sizeOfVarLong(long value) {
        return VarLong.sizeOf(value);
    }

    public static java.lang.String decodeString(ByteBuffer buffer) {
        return String.decode(buffer);
    }

    public static void encodeString(java.lang.String value, ResponseBuffer buffer) {
        String.encode(value, buffer);
    }

    public static java.lang.String decodeCompactString(ByteBuffer buffer) {
        return CompactString.decode(buffer);
    }

    public static void encodeCompactString(java.lang.String value, ResponseBuffer buffer) {
        CompactString.encode(value, buffer);
    }

    public static byte[] decodeBytes(ByteBuffer buffer) {
        return Bytes.decode(buffer);
    }

    public static void encodeBytes(byte[] value, ResponseBuffer buffer) {
        Bytes.encode(value, buffer);
    }

    public static Optional<byte[]> decodeNullableBytes(ByteBuffer buffer) {
        return NullableBytes.decode(buffer);
    }

    public static void encodeNullableBytes(Optional<byte[]> value, ResponseBuffer buffer) {
        NullableBytes.encode(value, buffer);
    }
}
