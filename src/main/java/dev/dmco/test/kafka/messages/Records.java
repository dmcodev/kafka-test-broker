package dev.dmco.test.kafka.messages;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.experimental.Accessors;

import java.util.List;

@lombok.Value
@Builder
@AllArgsConstructor
@Accessors(fluent = true)
public class Records {

    int version;

    @Singular
    List<Record> records;

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Record {

        long offset;

        byte[] key;

        byte[] value;
    }

}
