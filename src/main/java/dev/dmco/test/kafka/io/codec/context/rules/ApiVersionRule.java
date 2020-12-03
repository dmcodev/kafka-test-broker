package dev.dmco.test.kafka.io.codec.context.rules;

import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import lombok.RequiredArgsConstructor;

import static dev.dmco.test.kafka.io.codec.context.ContextProperty.API_VERSION;
import static dev.dmco.test.kafka.io.codec.context.ContextProperty.EXCLUDE_FIELD;

@RequiredArgsConstructor
public class ApiVersionRule implements CodecRule {

    private final int min;
    private final int max;

    public static ApiVersionRule from(ApiVersion metadata) {
        return new ApiVersionRule(metadata.min(), metadata.max());
    }

    @Override
    public boolean applies(CodecContext context) {
        int apiVersion = context.get(API_VERSION);
        return apiVersion >= min && apiVersion <= max;
    }

    @Override
    public CodecContext apply(CodecContext context) {
        return context.set(EXCLUDE_FIELD, true);
    }
}
