package dev.dmco.test.kafka.io.codec.context.rules;

import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MapVersionRule implements CodecRule {

    private final int version;
    private final int sinceVersion;

    public static MapVersionRule from(VersionMapping metadata) {
        return new MapVersionRule(metadata.value(), metadata.sinceVersion());
    }

    @Override
    public boolean applies(CodecContext context) {
        return context.version() >= sinceVersion;
    }

    @Override
    public CodecContext apply(CodecContext context) {
        return context.withVersion(version);
    }
}
