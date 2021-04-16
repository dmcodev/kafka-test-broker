package dev.dmcode.test.kafka.io.codec.specific;

import dev.dmcode.test.kafka.io.buffer.ResponseBuffer;
import dev.dmcode.test.kafka.io.codec.Codec;
import dev.dmcode.test.kafka.io.codec.context.CodecContext;
import dev.dmcode.test.kafka.io.codec.registry.Type;
import dev.dmcode.test.kafka.io.protocol.Protocol;
import dev.dmcode.test.kafka.messages.Tag;

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
        int numberOfFields = Protocol.decodeVarUInt(buffer);
        if (numberOfFields > 0) {
            tags = new ArrayList<>();
        }
        for (int i = 0; i < numberOfFields; i++) {
            int key = Protocol.decodeVarUInt(buffer);
            int size = Protocol.decodeVarUInt(buffer);
            byte[] value = new byte[size];
            buffer.get(value);
            tags.add(new Tag(key, value));
        }
        return tags;
    }

    @Override
    public void encode(Object value, Type valueType, ResponseBuffer buffer, CodecContext context) {
        List<Tag> tags = (List<Tag>) value;
        Protocol.encodeVarUInt(tags.size(), buffer);
        for (Tag tag : tags) {
            Protocol.encodeVarUInt(tag.key(), buffer);
            Protocol.encodeVarUInt(tag.value().length, buffer);
            buffer.putBytes(tag.value());
        }
    }

    @Override
    public void encodeNull(Type valueType, ResponseBuffer buffer, CodecContext context) {
        encode(Collections.emptyList(), valueType, buffer, context);
    }
}
