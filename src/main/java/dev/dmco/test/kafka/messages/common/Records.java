package dev.dmco.test.kafka.messages.common;

import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Accessors(fluent = true)
public class Records {
    List<Record> records;
}
