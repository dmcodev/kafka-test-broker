package dev.dmco.test.kafka.io.codec.context.rules;

import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.messages.metadata.SinceVersion;
import lombok.RequiredArgsConstructor;

import static dev.dmco.test.kafka.io.codec.context.ContextProperty.EXCLUDE_FIELD;
import static dev.dmco.test.kafka.io.codec.context.ContextProperty.VERSION;

@RequiredArgsConstructor
public class ExcludeFieldFromApiVersionRule implements CodecRule {

    private final int min;

    public static ExcludeFieldFromApiVersionRule from(SinceVersion metadata) {
        return new ExcludeFieldFromApiVersionRule(metadata.value());
    }

    @Override
    public boolean applies(CodecContext context) {
        return context.get(VERSION) < min;
    }

    @Override
    public CodecContext apply(CodecContext context) {
        return context.set(EXCLUDE_FIELD, true);
    }
}
