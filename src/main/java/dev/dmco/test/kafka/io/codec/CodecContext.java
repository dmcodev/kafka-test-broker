package dev.dmco.test.kafka.io.codec;

import dev.dmco.test.kafka.io.codec.struct.StructCodec;
import lombok.Builder;
import lombok.Value;
import lombok.With;
import lombok.experimental.Accessors;

@Value
@With
@Builder
@Accessors(fluent = true)
public class CodecContext {
    int apiVersion;
    Class<?> messageType;
    StructCodec structCodec;
}
