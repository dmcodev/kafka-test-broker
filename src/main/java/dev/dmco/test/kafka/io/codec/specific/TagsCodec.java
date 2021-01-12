package dev.dmco.test.kafka.io.codec.specific;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.registry.Type;
import dev.dmco.test.kafka.messages.Tag;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static dev.dmco.test.kafka.io.protocol.Protocol.decodeVarUInt;
import static dev.dmco.test.kafka.io.protocol.Protocol.encodeVarUInt;

public class TagsCodec implements Codec {

    @Override
    public Stream<Type> handledTypes() {
        return Stream.of(Type.of(Collection.class, Type.of(Tag.class)));
    }

    @Override
    public Object decode(ByteBuffer buffer, Type targetType, CodecContext context) {
        List<Tag> tags = Collections.emptyList();
        int numberOfFields = decodeVarUInt(buffer);
        if (numberOfFields > 0) {
            tags = new ArrayList<>();
        }
        for (int i = 0; i < numberOfFields; i++) {
            int key = decodeVarUInt(buffer);
            int size = decodeVarUInt(buffer);
            byte[] value = new byte[size];
            buffer.get(value);
            tags.add(new Tag(key, value));
        }
        return tags;
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        List<Tag> tags = (List<Tag>) value;
        encodeVarUInt(tags.size(), buffer);
        for (Tag tag : tags) {
            encodeVarUInt(tag.key(), buffer);
            encodeVarUInt(tag.value().length, buffer);
            buffer.putBytes(tag.value());
        }
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(Collections.emptyList(), valueType, buffer, context);
    }
}
