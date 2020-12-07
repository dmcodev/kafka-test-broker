package dev.dmco.test.kafka.io.codec.structs;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.primitives.VarUInt;
import dev.dmco.test.kafka.io.codec.registry.Type;
import dev.dmco.test.kafka.messages.Tag;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class TagsCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(Collection.class, Type.of(Tag.class)));
    }

    @Override
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        List<Tag> tags = Collections.emptyList();
        int numberOfFields = VarUInt.decode(buffer);
        if (numberOfFields > 0) {
            tags = new ArrayList<>();
        }
        for (int i = 0; i < numberOfFields; i++) {
            int key = VarUInt.decode(buffer);
            int size = VarUInt.decode(buffer);
            byte[] value = new byte[size];
            buffer.get(value);
            tags.add(new Tag(key, value));
        }
        return tags;
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        if (value != null) {
            List<Tag> tags = (List<Tag>) value;
            VarUInt.encode(tags.size(), buffer);
            for (Tag tag : tags) {
                VarUInt.encode(tag.key(), buffer);
                VarUInt.encode(tag.value().length, buffer);
                buffer.putBytes(tag.value());
            }
        } else {
            VarUInt.encode(0, buffer);
        }
    }
}
