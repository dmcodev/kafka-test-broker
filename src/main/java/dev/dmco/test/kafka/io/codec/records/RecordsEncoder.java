package dev.dmco.test.kafka.io.codec.records;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.messages.Record;

import java.util.Collection;

class RecordsEncoder {

    static void encode(Collection<Record> records, ResponseBuffer buffer) {
        // TODO
        LegacyRecordsEncoder.encode(records, buffer, 1);
    }
}
