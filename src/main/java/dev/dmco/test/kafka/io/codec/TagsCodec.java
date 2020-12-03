package dev.dmco.test.kafka.io.codec;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.messages.common.Tag;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static dev.dmco.test.kafka.io.codec.registry.CodecRegistry.UVAR_INT;

public class TagsCodec implements Codec {

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        List<Tag> tags = Collections.emptyList();
        int numberOfFields = UVAR_INT.decode(buffer, context);
        if (numberOfFields > 0) {
            tags = new ArrayList<>();
        }
        for (int i = 0; i < numberOfFields; i++) {
            int key = UVAR_INT.decode(buffer, context);
            int size = UVAR_INT.decode(buffer, context);
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
            UVAR_INT.encode(tags.size(), buffer, context);
            for (Tag tag : tags) {
                UVAR_INT.encode(tag.key(), buffer, context);
                UVAR_INT.encode(tag.value().length, buffer, context);
                buffer.putBytes(tag.value());
            }
        } else {
            UVAR_INT.encode(0, buffer, context);
        }
    }
}
