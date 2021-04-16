package dev.dmcode.test.kafka.io.codec.context.rules;

import dev.dmcode.test.kafka.io.codec.context.CodecContext;

public interface CodecRule {

    boolean applies(CodecContext context);

    CodecContext apply(CodecContext context);
}
