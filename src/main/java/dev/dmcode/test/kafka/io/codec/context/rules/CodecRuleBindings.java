package dev.dmcode.test.kafka.io.codec.context.rules;

import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.metadata.VersionMappings;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CodecRuleBindings {

    private static final Map<Class<?>, Function<Object, CodecRule>> BINDINGS = new HashMap<>();

    static {
        addBinding(SinceVersion.class, VersionRule::from);
        addBinding(VersionMapping.class, MapVersionRule::from);
        addBinding(VersionMappings.class, MapVersionRule::from);
    }

    public static Collection<CodecRule> createRules(Annotation[] metadata) {
        return Arrays.stream(metadata)
            .map(CodecRuleBindings::createRule)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    }

    private static Optional<CodecRule> createRule(Annotation metadata) {
        Class<?> metadataType = metadata.annotationType();
        return Optional.ofNullable(BINDINGS.get(metadataType))
            .map(factory -> factory.apply(metadata));
    }

    @SuppressWarnings("unchecked")
    private static <T> void addBinding(Class<T> metadataType, Function<T, CodecRule> mapping) {
        BINDINGS.put(metadataType, (Function<Object, CodecRule>) mapping);
    }
}
