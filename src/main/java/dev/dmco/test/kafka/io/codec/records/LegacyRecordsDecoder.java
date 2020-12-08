package dev.dmco.test.kafka.io.codec.records;

import dev.dmco.test.kafka.io.codec.bytes.BytesCodec;
import dev.dmco.test.kafka.messages.Record;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class LegacyRecordsDecoder {

    private static final int RECORD_ATTRIBUTES_OFFSET = 17;
    private static final int RECORD_TIMESTAMP_OFFSET = RECORD_ATTRIBUTES_OFFSET + 1;

    static List<Record> decode(ByteBuffer buffer, int version, int length) {
        Compression compression = getCompressionType(buffer);
        if (compression == Compression.NONE) {
            return decodePlain(buffer, version, length);
        }
        return decodeCompressed(buffer, version, compression);
    }

    private static List<Record> decodePlain(ByteBuffer buffer, int version, int length) {
        int recordsEndOffset = buffer.position() + length;
        List<Record> records = new ArrayList<>();
        while (buffer.position() < recordsEndOffset) {
            int recordStartOffset = buffer.position();
            long offset = buffer.getLong();
            buffer.position(recordStartOffset + RECORD_TIMESTAMP_OFFSET);
            if (version == 1) {
                buffer.getLong();
            }
            Record record = Record.builder()
                .offset(offset)
                .key(BytesCodec.decode(buffer))
                .value(BytesCodec.decode(buffer))
                .build();
            records.add(record);
        }
        return records;
    }

    private static List<Record> decodeCompressed(ByteBuffer buffer, int version, Compression compression) {
        buffer.position(buffer.position() + RECORD_TIMESTAMP_OFFSET);
        if (version == 1) {
            buffer.getLong();
        }
        buffer.getInt();
        byte[] compressedRecordsBytes = BytesCodec.decode(buffer);
        ByteBuffer decompressedRecords = ByteBuffer.wrap(compression.decompress(compressedRecordsBytes));
        return decodePlain(decompressedRecords, version, decompressedRecords.remaining());
    }

    private static Compression getCompressionType(ByteBuffer buffer) {
        byte attributes = buffer.get(buffer.position() + RECORD_ATTRIBUTES_OFFSET);
        return Compression.from(attributes);
    }
}
