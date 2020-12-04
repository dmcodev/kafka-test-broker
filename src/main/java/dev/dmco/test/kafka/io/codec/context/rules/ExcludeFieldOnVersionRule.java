package dev.dmco.test.kafka.io.codec.context.rules;

import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.messages.metadata.SinceVersion;
import lombok.RequiredArgsConstructor;

import static dev.dmco.test.kafka.io.codec.context.ContextProperty.EXCLUDE_FIELD;
import static dev.dmco.test.kafka.io.codec.context.ContextProperty.VERSION;

@RequiredArgsConstructor
public class ExcludeFieldOnVersionRule implements CodecRule {

    private final int min;

    public static ExcludeFieldOnVersionRule from(SinceVersion metadata) {
        return new ExcludeFieldOnVersionRule(metadata.value());
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
