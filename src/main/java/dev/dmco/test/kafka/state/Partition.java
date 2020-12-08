package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.messages.Record;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
class Partition {

    private final Map<Long, Record> records = new HashMap<>();
    private final int id;

    private long head = 0;

    void append(Collection<Record> records) {
        records.forEach(this::append);
    }

    private void append(Record record) {
        records.put(head, record.withOffset(head));
        head++;
    }
}
