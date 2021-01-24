package dev.dmco.test.kafka.io.codec.records;

import dev.dmco.test.kafka.messages.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static dev.dmco.test.kafka.io.protocol.Protocol.decodeVarInt;

class RecordsDecoder {

    private static final int BASE_OFFSET_OFFSET = 0;
    private static final int ATTRIBUTES_OFFSET = 21;
    private static final int RECORDS_COUNT_OFFSET = ATTRIBUTES_OFFSET + 36;
    private static final int RECORDS_OFFSET = RECORDS_COUNT_OFFSET + 4;

    static Collection<Record> decode(ByteBuffer buffer, int length) {
        long baseOffset = buffer.getLong(buffer.position() + BASE_OFFSET_OFFSET);
        Compression compression = getCompressionType(buffer);
        int count = buffer.getInt(buffer.position() + RECORDS_COUNT_OFFSET);
        buffer.position(buffer.position() + RECORDS_OFFSET);
        if (compression == Compression.NONE) {
            return decodeRecords(buffer, count, baseOffset);
        }
        return decodeCompressed(buffer, count, baseOffset, length, compression);
    }

    private static Collection<Record> decodeRecords(ByteBuffer buffer, int count, long baseOffset) {
        List<Record> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            decodeVarInt(buffer);
            buffer.get();
            decodeVarInt(buffer);
            Record record = Record.builder()
                .offset(baseOffset + decodeVarInt(buffer))
                .key(decodeOptionalBytes(buffer))
                .value(decodeOptionalBytes(buffer))
                .headers(decodeHeaders(buffer))
                .build();
            records.add(record);
        }
        return records;
    }

    private static Collection<Record.Header> decodeHeaders(ByteBuffer buffer) {
        int size = decodeVarInt(buffer);
        Collection<Record.Header> headers = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            byte[] keyBytes = new byte[decodeVarInt(buffer)];
            buffer.get(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);
            Record.Header header = Record.Header.builder()
                .key(key)
                .value(decodeOptionalBytes(buffer))
                .build();
            headers.add(header);
        }
        return headers;
    }

    private static Collection<Record> decodeCompressed(ByteBuffer buffer, int count, long baseOffset, int length, Compression compression) {
        byte[] compressedRecordsBytes = new byte[length - RECORDS_OFFSET];
        buffer.get(compressedRecordsBytes);
        ByteBuffer decompressedRecordsBytes = ByteBuffer.wrap(compression.decompress(compressedRecordsBytes));
        return decodeRecords(decompressedRecordsBytes, count, baseOffset);
    }

    private static Compression getCompressionType(ByteBuffer buffer) {
        short attributes = buffer.getShort(buffer.position() + ATTRIBUTES_OFFSET);
        return Compression.from(attributes);
    }

    private static Optional<byte[]> decodeOptionalBytes(ByteBuffer buffer) {
        int length = decodeVarInt(buffer);
        byte[] value = null;
        if (length >= 0) {
            value = new byte[length];
            buffer.get(value);
        }
        return Optional.ofNullable(value);
    }
}
