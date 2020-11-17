package dev.dmco.test.kafka.io.codec.value;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.CodecContext;
import dev.dmco.test.kafka.messages.common.Tag;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TagsCodec implements ValueTypeCodec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        List<Tag> tags = Collections.emptyList();
        int numberOfFields = decodeUVarInt(buffer, context);
        if (numberOfFields > 0) {
            tags = new ArrayList<>();
        }
        for (int i = 0; i < numberOfFields; i++) {
            int key = decodeUVarInt(buffer, context);
            int size = decodeUVarInt(buffer, context);
            byte[] value = new byte[size];
            buffer.get(value);
            tags.add(new Tag(key, value));
        }
        return tags;
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        if (value != null) {
            List<Tag> tags = (List<Tag>) value;
            encodeUVarInt(tags.size(), buffer, context);
            for (Tag tag : tags) {
                encodeUVarInt(tag.key(), buffer, context);
                encodeUVarInt(tag.value().length, buffer, context);
                buffer.putBytes(tag.value());
            }
        } else {
            encodeUVarInt(0, buffer, context);
        }
    }
}
