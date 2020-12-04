package dev.dmco.test.kafka.io.codec.context.rules;

import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import lombok.RequiredArgsConstructor;

import static dev.dmco.test.kafka.io.codec.context.ContextProperty.VERSION;

@RequiredArgsConstructor
public class MapApiVersionRule implements CodecRule {

    private final int apiVersion;
    private final int sinceApiVersion;

    public static MapApiVersionRule from(VersionMapping metadata) {
        return new MapApiVersionRule(metadata.value(), metadata.sinceApiVersion());
    }

    @Override
    public boolean applies(CodecContext context) {
        return context.get(VERSION) >= sinceApiVersion;
    }

    @Override
    public CodecContext apply(CodecContext context) {
        return context.set(VERSION, apiVersion);
    }
}
