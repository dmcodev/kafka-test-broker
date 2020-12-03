package dev.dmco.test.kafka.io.codec.context.rules;

import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.messages.metadata.ApiVersionMapping;
import lombok.RequiredArgsConstructor;

import static dev.dmco.test.kafka.io.codec.context.ContextProperty.API_VERSION;

@RequiredArgsConstructor
public class MapApiVersionRule implements CodecRule {

    private final int apiVersion;
    private final int sinceApiVersion;

    public static MapApiVersionRule from(ApiVersionMapping metadata) {
        return new MapApiVersionRule(metadata.value(), metadata.sinceApiVersion());
    }

    @Override
    public boolean applies(CodecContext context) {
        return context.get(API_VERSION) >= sinceApiVersion;
    }

    @Override
    public CodecContext apply(CodecContext context) {
        return context.set(API_VERSION, apiVersion);
    }
}
