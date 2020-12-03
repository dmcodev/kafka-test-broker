package dev.dmco.test.kafka.io.codec.context.rules.binding;

import dev.dmco.test.kafka.io.codec.context.rules.CodecRule;

import java.util.Collection;
import java.util.function.Function;

@FunctionalInterface
public interface CodecRuleBinding<T> extends Function<T, Collection<CodecRule>> {}
