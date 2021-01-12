package dev.dmco.test.kafka.io.codec.specific;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;
import dev.dmco.test.kafka.messages.consumer.Assignment;

public class AssignmentCodec extends ConsumerMessageCodec {

    public AssignmentCodec() {
        super(Assignment.class);
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(Assignment.builder().build(), valueType, buffer, context);
    }
}
