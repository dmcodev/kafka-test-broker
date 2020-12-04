package dev.dmco.test.kafka.messages;

import lombok.AllArgsConstructor;
import lombok.experimental.Accessors;

@lombok.Value
@AllArgsConstructor
@Accessors(fluent = true)
public class Tag {
    int key;
    byte[] value;
}
