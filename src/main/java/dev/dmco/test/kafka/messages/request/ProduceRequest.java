package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.io.struct.FieldType;
import dev.dmco.test.kafka.messages.RequestMessage;
import dev.dmco.test.kafka.messages.data.TopicData;
import dev.dmco.test.kafka.messages.meta.Field;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Accessors(fluent = true)
@Request(apiKey = 0)
public class ProduceRequest implements RequestMessage {

    RequestHeader header;

    @Field(FieldType.INT16)
    Integer acknowledgments;

    @Field(FieldType.INT32)
    Integer timeout;

    @StructSequence(TopicData.class)
    List<TopicData> topicData;
}
