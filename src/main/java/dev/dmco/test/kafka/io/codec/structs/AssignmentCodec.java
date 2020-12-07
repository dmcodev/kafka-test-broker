package dev.dmco.test.kafka.io.codec.structs;

import dev.dmco.test.kafka.io.codec.generic.VersionedBytesCodec;
import dev.dmco.test.kafka.messages.Assignment;

public class AssignmentCodec extends VersionedBytesCodec {

    public AssignmentCodec() {
        super(Assignment.class);
    }
}
