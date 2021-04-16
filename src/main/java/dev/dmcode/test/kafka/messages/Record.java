package dev.dmcode.test.kafka.messages;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.Optional;

@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class Record {

    long offset;

    Optional<byte[]> key;

    Optional<byte[]> value;

    Collection<Header> headers;

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Header {

        String key;

        Optional<byte[]> value;
    }
}
