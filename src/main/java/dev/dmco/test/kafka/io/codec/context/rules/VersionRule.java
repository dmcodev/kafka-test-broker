package dev.dmco.test.kafka.io.codec.context.rules;

import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.messages.metadata.SinceVersion;
import lombok.RequiredArgsConstructor;

import static dev.dmco.test.kafka.io.codec.context.ContextProperty.VERSION;
import static dev.dmco.test.kafka.io.codec.context.ContextProperty.VERSION_MISMATCH;

@RequiredArgsConstructor
public class VersionRule implements CodecRule {

    private final int min;

    public static VersionRule from(SinceVersion metadata) {
        return new VersionRule(metadata.value());
    }

    @Override
    public boolean applies(CodecContext context) {
        return context.get(VERSION) < min;
    }

    @Override
    public CodecContext apply(CodecContext context) {
        return context.set(VERSION_MISMATCH, true);
    }
}
