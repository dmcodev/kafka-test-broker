package dev.dmcode.test.kafka.io.codec.context.rules;

import dev.dmcode.test.kafka.io.codec.context.CodecContext;

import java.util.Collection;

public interface CodecRulesAware {

    Collection<CodecRule> codecRules();

    default CodecContext createContextFromRules(CodecContext initialContext) {
        CodecContext context = initialContext;
        for (CodecRule rule : codecRules()) {
            if (rule.applies(context)) {
                context = rule.apply(context);
            }
        }
        return context;
    }
}
