package dev.dmco.test.kafka.messages;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.Optional;

@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class Record {

    long offset;

    Optional<byte[]> key;

    byte[] value;
}
