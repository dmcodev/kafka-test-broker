package dev.dmco.test.kafka.messages;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.With;
import lombok.experimental.Accessors;

@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class Record {

    long offset;

    byte[] key;

    byte[] value;
}
