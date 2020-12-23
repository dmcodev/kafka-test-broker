package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.messages.Record;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
@Accessors(fluent = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Partition {

    private final Map<Long, Record> records = new HashMap<>();

    @Getter
    @EqualsAndHashCode.Include
    private final Topic topic;

    @Getter
    @EqualsAndHashCode.Include
    private final int id;

    @Getter
    private long head = 0;

    public AppendResult append(Collection<Record> records) {
        long baseOffset = head;
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
            Record record = records.get(offset);
            result.add(record);
            resultSize += record.key().length;
            resultSize += record.value().length;
            offset++;
        } while (resultSize < maxFetchSizeInBytes && records.containsKey(offset));
        return result;
    }

    private void append(Record record) {
        records.put(head, record.withOffset(head));
        head++;
    }

    @Value
    @Builder
    @Accessors(fluent = true)
    public static class AppendResult {
        long baseOffset;
    }
}
