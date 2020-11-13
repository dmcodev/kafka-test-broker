package dev.dmco.test.kafka.messages.common;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class Record {
    byte[] key;
    byte[] body;
}
