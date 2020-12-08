package dev.dmco.test.kafka.io.codec.records;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.bytes.BytesCodec;
import dev.dmco.test.kafka.messages.Records;
import dev.dmco.test.kafka.messages.Records.Record;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32;

class LegacyRecordsEncoder {

    static void encode(Records records, ResponseBuffer buffer) {
        encode(records.records(), buffer, records.version());
    }

    private static void encode(List<Record> records, ResponseBuffer buffer, int version) {
        ByteBuffer recordsSizeSlot = buffer.putIntSlot();
        int recordsStartOffset = buffer.position();
        for (Record record : records) {
            encode(record, buffer, version);
        }
        int recordsSize = buffer.position() - recordsStartOffset;
        recordsSizeSlot.putInt(recordsSize);
    }

    private static void encode(Record record, ResponseBuffer buffer, int version) {
        buffer.putLong(record.offset());
        ByteBuffer sizeSlot = buffer.putIntSlot();
        int recordStartOffset = buffer.position();
        ByteBuffer checkSumSlot = buffer.putIntSlot();
        int checksumInputStartOffset = buffer.position();
        buffer.putByte((byte) version);
        buffer.putByte((byte) 0);
        if (version == 1) {
            buffer.putLong(System.currentTimeMillis());
        }
        BytesCodec.encode(record.key(), buffer);
        BytesCodec.encode(record.value(), buffer);
        int recordEndOffset = buffer.position();
        byte[] checksumInput = buffer.read(checksumInputStartOffset, recordEndOffset - checksumInputStartOffset);
        int checksum = computeChecksum(checksumInput);
        checkSumSlot.putInt(checksum);
        int size = recordEndOffset - recordStartOffset;
        sizeSlot.putInt(size);
    }

    private static int computeChecksum(byte[] bytes) {
        CRC32 checksum = new CRC32();
        checksum.update(bytes);
        return (int) checksum.getValue();
    }
}