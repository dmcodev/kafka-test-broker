package dev.dmcode.test.kafka.io.codec.context.rules;

import dev.dmcode.test.kafka.io.codec.context.CodecContext;
import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class VersionRule implements CodecRule {

    private final int min;

    public static VersionRule from(SinceVersion metadata) {
        return new VersionRule(metadata.value());
    }

    @Override
    public boolean applies(CodecContext context) {
        return context.version() < min;
    }

    @Override
    public CodecContext apply(CodecContext context) {
        return context.withSkipProperty(true);
    }
}
