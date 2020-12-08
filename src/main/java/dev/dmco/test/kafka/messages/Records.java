package dev.dmco.test.kafka.messages;

import dev.dmco.test.kafka.messages.metadata.VersionMapping;
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

    @VersionMapping(value = 0, sinceVersion = 0)
    @VersionMapping(value = 1, sinceVersion = 2)
    @VersionMapping(value = 2, sinceVersion = 3)
    @Singular
    List<Record> entries;
}
