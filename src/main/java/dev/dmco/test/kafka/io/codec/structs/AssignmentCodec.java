package dev.dmco.test.kafka.io.codec.structs;

import dev.dmco.test.kafka.io.codec.generic.VersionedTypeBytesCodec;
import dev.dmco.test.kafka.messages.Assignment;

public class AssignmentCodec extends VersionedTypeBytesCodec {

    public AssignmentCodec() {
        super(Assignment.class);
    }
}
