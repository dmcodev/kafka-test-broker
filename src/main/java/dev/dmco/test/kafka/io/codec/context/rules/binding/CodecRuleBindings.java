package dev.dmco.test.kafka.io.codec.context.rules.binding;

import dev.dmco.test.kafka.io.codec.context.rules.CodecRule;
import dev.dmco.test.kafka.io.codec.context.rules.ExcludeFieldFromApiVersionRule;
import dev.dmco.test.kafka.io.codec.context.rules.MapApiVersionRule;
import dev.dmco.test.kafka.messages.metadata.ApiVersionMapping;
import dev.dmco.test.kafka.messages.metadata.ApiVersionMappings;
import dev.dmco.test.kafka.messages.metadata.SinceApiVersion;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CodecRuleBindings {

    private static final Map<Class<?>, CodecRuleBinding<Object>> BINDINGS = new HashMap<>();

    static  {
        addBinding(SinceApiVersion.class, single(ExcludeFieldFromApiVersionRule::from));
        addBinding(ApiVersionMapping.class, single(MapApiVersionRule::from));
        addBinding(ApiVersionMappings.class, compound(ApiVersionMappings::value, MapApiVersionRule::from));
    }

    public static Collection<CodecRule> createRules(Annotation metadata) {
        Class<?> metadataType = metadata.annotationType();
        return Optional.ofNullable(BINDINGS.get(metadataType))
            .map(binding -> binding.apply(metadata))
            .orElseGet(Collections::emptyList);
    }

    @SuppressWarnings("unchecked")
    private static <T> void addBinding(Class<T> metadataType, CodecRuleBinding<T> binding) {
        BINDINGS.put(metadataType, (CodecRuleBinding<Object>) binding);
    }

    private static <T> CodecRuleBinding<T> single(Function<T, CodecRule> mapping) {
        return metadata -> Collections.singletonList(mapping.apply(metadata));
    }

    private static <C, T> CodecRuleBinding<C> compound(Function<C, T[]> unpack, Function<T, CodecRule> mapping) {
        return metadata -> Arrays.stream(unpack.apply(metadata)).map(mapping).collect(Collectors.toList());
    }
}