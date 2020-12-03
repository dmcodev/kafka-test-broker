package dev.dmco.test.kafka.io.codec.context.rules;

import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.messages.metadata.SinceApiVersion;
import lombok.RequiredArgsConstructor;

import static dev.dmco.test.kafka.io.codec.context.ContextProperty.API_VERSION;
import static dev.dmco.test.kafka.io.codec.context.ContextProperty.EXCLUDE_FIELD;

@RequiredArgsConstructor
public class ExcludeFieldFromApiVersionRule implements CodecRule {

    private final int min;

    public static ExcludeFieldFromApiVersionRule from(SinceApiVersion metadata) {
        return new ExcludeFieldFromApiVersionRule(metadata.value());
    }

    @Override
    public boolean applies(CodecContext context) {
        return context.get(API_VERSION) < min;
    }

    @Override
    public CodecContext apply(CodecContext context) {
        return context.set(EXCLUDE_FIELD, true);
    }
}
