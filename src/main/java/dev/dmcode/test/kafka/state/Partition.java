package dev.dmcode.test.kafka.state;

import dev.dmcode.test.kafka.messages.Record;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@ToString(onlyExplicitlyIncluded = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Partition {

    private final Map<Long, Record> records = new HashMap<>();

    @ToString.Include
    @EqualsAndHashCode.Include
    private final int id;

    @ToString.Include
    @EqualsAndHashCode.Include
    private final Topic topic;

    private long headOffset = 0;

    public AppendResult append(Collection<Record> records) {
        long baseOffset = headOffset;
        records.forEach(this::append);
        return AppendResult.builder()
            .baseOffset(baseOffset)
            .build();
    }

    public List<Record> fetch(long startOffset, int maxFetchSizeInBytes) {
        if (!records.containsKey(startOffset)) {
            return Collections.emptyList();
        }
        List<Record> result = new ArrayList<>();
        long offset = startOffset;
        int resultSize = 0;
        do {
            Record record = records.get(offset++);
            int recordSize = record.key().map(key -> key.length).orElse(0)
                + record.value().map(value -> value.length).orElse(0);
            if (result.size() > 0 && resultSize + recordSize > maxFetchSizeInBytes) {
                break;
            }
            result.add(record);
            resultSize += recordSize;
        } while (records.containsKey(offset));
        return result;
    }

    public Collection<Record> getRecords() {
        return records.values();
    }

    public long getHeadOffset() {
        return headOffset;
    }

    public Topic getTopic() {
        return topic;
    }

    public int getId() {
        return id;
    }

    private void append(Record record) {
        records.put(headOffset, record.withOffset(headOffset));
        headOffset++;
    }

    @Value
    @Builder
    @Accessors(fluent = true)
    public static class AppendResult {
        long baseOffset;
    }
}
