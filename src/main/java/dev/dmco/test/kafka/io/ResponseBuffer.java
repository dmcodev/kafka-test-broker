package dev.dmco.test.kafka.io;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

import java.nio.ByteBuffer;

@Value
@Builder
@Accessors(fluent = true)
class ResponseBuffer {
    ByteBuffer header;
    ByteBuffer body;
}
