package dev.dmco.test.kafka.messages.data;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class TaggedFields {
    byte padding = 0;
}
