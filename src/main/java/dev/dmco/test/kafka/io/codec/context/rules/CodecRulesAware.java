package dev.dmco.test.kafka.io.codec.context.rules;

import dev.dmco.test.kafka.io.codec.context.CodecContext;

import java.util.Collection;

public interface CodecRulesAware {

    Collection<CodecRule> codecRules();

    default CodecContext createContext(CodecContext initialContext) {
        return codecRules().stream()
            .reduce(
                initialContext,
                (context, codecRule) -> codecRule.applies(context) ? codecRule.apply(context) : context,
                CodecContext::merge
            );
    }
}
