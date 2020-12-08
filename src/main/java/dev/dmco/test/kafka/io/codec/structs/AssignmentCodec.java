package dev.dmco.test.kafka.io.codec.structs;

import dev.dmco.test.kafka.messages.Assignment;

public class AssignmentCodec extends VersionedCodec {

    public AssignmentCodec() {
        super(Assignment.class);
    }
}
