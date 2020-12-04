package dev.dmco.test.kafka.io.codec.structs;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.messages.Tag;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static dev.dmco.test.kafka.io.codec.registry.CodecRegistry.VAR_UINT;

public class TagsCodec implements Codec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        List<Tag> tags = Collections.emptyList();
        int numberOfFields = VAR_UINT.decode(buffer, context);
        if (numberOfFields > 0) {
            tags = new ArrayList<>();
        }
        for (int i = 0; i < numberOfFields; i++) {
            int key = VAR_UINT.decode(buffer, context);
            int size = VAR_UINT.decode(buffer, context);
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
            VAR_UINT.encode(tags.size(), buffer, context);
            for (Tag tag : tags) {
                VAR_UINT.encode(tag.key(), buffer, context);
                VAR_UINT.encode(tag.value().length, buffer, context);
                buffer.putBytes(tag.value());
            }
        } else {
            VAR_UINT.encode(0, buffer, context);
        }
    }
}
