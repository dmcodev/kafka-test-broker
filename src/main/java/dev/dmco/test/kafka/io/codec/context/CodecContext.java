package dev.dmco.test.kafka.io.codec.context;

import lombok.Builder;
import lombok.Value;
import lombok.With;
import lombok.experimental.Accessors;

@Value
@With
@Builder
@Accessors(fluent = true)
public class CodecContext {
    int version;
    boolean skipProperty;
}
