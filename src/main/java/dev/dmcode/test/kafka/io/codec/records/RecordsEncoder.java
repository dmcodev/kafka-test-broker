package dev.dmcode.test.kafka.io.codec.records;

import dev.dmcode.test.kafka.io.buffer.ResponseBuffer;
import dev.dmcode.test.kafka.io.protocol.Protocol;
import dev.dmcode.test.kafka.messages.Record;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

class RecordsEncoder {

    static void encode(Collection<Record> records, ResponseBuffer buffer) {
        ByteBuffer recordsSizeSlot = buffer.putIntSlot();
        int recordsStartOffset = buffer.position();
        long baseOffset = records.stream()
            .mapToLong(Record::offset)
            .min()
            .orElse(0L);
        buffer.putLong(baseOffset); // base offset
        ByteBuffer batchLengthSlot = buffer.putIntSlot();
        int batchStartOffset = buffer.position();
        buffer.putInt(-1); // partitionLeaderEpoch
        buffer.putByte((byte) 2); // magic
        ByteBuffer checkSumSlot = buffer.putIntSlot();
        int checksumInputStartOffset = buffer.position();
        encodeBatchHeader(records, baseOffset, buffer);
        for (Record record : records) {
            encodeRecord(record, baseOffset, buffer);
        }
        ByteBuffer checksumInput = buffer.slice(checksumInputStartOffset, buffer.position() - checksumInputStartOffset);
        checkSumSlot.putInt(CRC32C.compute(checksumInput));
        batchLengthSlot.putInt(buffer.position() - batchStartOffset);
        recordsSizeSlot.putInt(buffer.position() - recordsStartOffset);
    }

    private static void encodeBatchHeader(Collection<Record> records, long baseOffset, ResponseBuffer buffer) {
        buffer.putShort((short) 0); // attributes
        buffer.putInt(computeLastOffsetDelta(records, baseOffset)); // lastOffsetDelta
        buffer.putLong(-1); // firstTimestamp
        buffer.putLong(-1); // maxTimestamp
        buffer.putLong(-1); // producerId
        buffer.putShort((short) -1); // producerEpoch
        buffer.putInt((short) -1); // baseSequence
        buffer.putInt(records.size());
    }

    private static void encodeRecord(Record record, long baseOffset, ResponseBuffer buffer) {
        int offsetDelta = (int) (record.offset() - baseOffset);
        long timestampDelta = 0;
        int keySize = record.key().map(it -> it.length).orElse(0);
        int valueSize = record.value().map(it -> it.length).orElse(0);
        int headersSize = computeHeadersSize(record.headers());
        int recordBodySize = Protocol.sizeOfVarInt(offsetDelta)
            + Protocol.sizeOfVarLong(timestampDelta)
            + Protocol.sizeOfVarInt(keySize) + keySize
            + Protocol.sizeOfVarInt(valueSize) + valueSize
            + headersSize + 1;
        Protocol.encodeVarInt(recordBodySize, buffer);
        buffer.putByte((byte) 0); // attributes
        Protocol.encodeVarLong(timestampDelta, buffer); // timestamp delta
        Protocol.encodeVarInt(offsetDelta, buffer); // offsetDelta
        encodeRecordKey(record, keySize, buffer);
        encodeRecordValue(record, valueSize, buffer);
        encodeHeaders(record.headers(), buffer);
    }

    private static void encodeRecordKey(Record record, int keySize, ResponseBuffer buffer) {
        if (keySize > 0) {
            Protocol.encodeVarInt(keySize, buffer);
            record.key().ifPresent(buffer::putBytes);
        } else {
            Protocol.encodeVarInt(-1, buffer);
        }
    }

    private static void encodeRecordValue(Record record, int valueSize, ResponseBuffer buffer) {
        if (valueSize > 0) {
            Protocol.encodeVarInt(valueSize, buffer);
            record.value().ifPresent(buffer::putBytes);
        } else {
            Protocol.encodeVarInt(-1, buffer);
        }
    }

    private static void encodeHeaders(Collection<Record.Header> headers, ResponseBuffer buffer) {
        Protocol.encodeVarInt(headers.size(), buffer);
        for (Record.Header header : headers) {
            byte[] keyBytes = header.key().getBytes(StandardCharsets.UTF_8);
            Protocol.encodeVarInt(keyBytes.length, buffer);
            buffer.putBytes(keyBytes);
            int valueSize = header.value().map(it -> it.length).orElse(-1);
            Protocol.encodeVarInt(valueSize, buffer);
            header.value().ifPresent(buffer::putBytes);
        }
    }

    private static int computeLastOffsetDelta(Collection<Record> records, long baseOffset) {
        return (int) (records.stream()
            .mapToLong(Record::offset)
            .max()
            .orElse(0L) - baseOffset);
    }

    private static int computeHeadersSize(Collection<Record.Header> headers) {
        int size = Protocol.sizeOfVarInt(headers.size());
        for (Record.Header header : headers) {
            byte[] keyBytes = header.key().getBytes(StandardCharsets.UTF_8);
            size += Protocol.sizeOfVarInt(keyBytes.length) + keyBytes.length;
            if (header.value().isPresent()) {
                byte[] valueBytes = header.value().get();
                size += Protocol.sizeOfVarInt(valueBytes.length) + valueBytes.length;
            } else {
                size += Protocol.sizeOfVarInt(-1);
            }
        }
        return size;
    }
}
