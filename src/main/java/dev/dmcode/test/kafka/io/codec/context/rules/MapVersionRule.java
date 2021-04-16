package dev.dmcode.test.kafka.io.codec.context.rules;

import dev.dmcode.test.kafka.io.codec.context.CodecContext;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.metadata.VersionMappings;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class MapVersionRule implements CodecRule {

    private final TreeMap<Integer, Integer> mapping;

    public MapVersionRule(Map<Integer, Integer> mapping) {
        this.mapping = new TreeMap<>(mapping);
    }

    public static MapVersionRule from(VersionMapping metadata) {
        return new MapVersionRule(Collections.singletonMap(metadata.sinceVersion(), metadata.value()));
    }

    public static MapVersionRule from(VersionMappings metadata) {
        Map<Integer, Integer> mapping = Arrays.stream(metadata.value())
            .collect(Collectors.toMap(VersionMapping::sinceVersion, VersionMapping::value));
        return new MapVersionRule(mapping);
    }

    @Override
    public boolean applies(CodecContext context) {
        return context.version() >= mapping.firstKey();
    }

    @Override
    public CodecContext apply(CodecContext context) {
        int mappedVersion = mapping.get(mapping.floorKey(context.version()));
        return context.withVersion(mappedVersion);
    }
}
