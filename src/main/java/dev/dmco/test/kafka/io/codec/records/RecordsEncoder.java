package dev.dmco.test.kafka.io.codec.records;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.messages.Record;

import java.nio.ByteBuffer;
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

        buffer.putShort((short) 0); // attributes
        buffer.putInt(-1); // lastOffsetDelta
        buffer.putLong(-1); // firstTimestamp
        buffer.putLong(-1); // maxTimestamp
        buffer.putLong(-1); // producerId
        buffer.putShort((short) -1); // producerEpoch
        buffer.putInt((short) -1); // baseSequence

        buffer.putInt(records.size());

        for (Record record : records) {
            encodeRecord(record, buffer);
        }

        ByteBuffer checksumInput = buffer.slice(checksumInputStartOffset, buffer.position() - checksumInputStartOffset);
        checkSumSlot.putInt(CRC32C.compute(checksumInput));

        batchLengthSlot.putInt(buffer.position() - batchStartOffset);
        recordsSizeSlot.putInt(buffer.position() - recordsStartOffset);
    }

    private static void encodeRecord(Record record, ResponseBuffer buffer) {
        //int sizeInBytes =
    }
}
