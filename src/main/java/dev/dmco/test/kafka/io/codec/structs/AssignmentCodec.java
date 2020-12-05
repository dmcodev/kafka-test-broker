package dev.dmco.test.kafka.io.codec.structs;

import dev.dmco.test.kafka.io.codec.bytes.VersionedBytesCodec;
import dev.dmco.test.kafka.io.codec.registry.TypeKey;
import dev.dmco.test.kafka.messages.Assignment;

import java.util.stream.Stream;

import static dev.dmco.test.kafka.io.codec.registry.TypeKey.key;

public class AssignmentCodec extends VersionedBytesCodec {

    public AssignmentCodec() {
        super(Assignment.class);
    }

    @Override
    public Stream<TypeKey> handledTypes() {
        return Stream.of(
            key(Assignment.class)
        );
    }
}
