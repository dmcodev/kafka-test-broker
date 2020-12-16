package dev.dmco.test.kafka.io.codec.consumer;

import dev.dmco.test.kafka.messages.consumer.Assignment;

public class AssignmentCodec extends ConsumerMessageCodec {

    public AssignmentCodec() {
        super(Assignment.class);
    }
}
