package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.messages.KafkaRequest;
import dev.dmco.test.kafka.messages.RequestMessage;
import dev.dmco.test.kafka.messages.data.TopicData;
import dev.dmco.test.kafka.messages.meta.Primitive;
import dev.dmco.test.kafka.messages.meta.PrimitiveType;
import dev.dmco.test.kafka.messages.meta.Sequence;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Accessors(fluent = true)
@KafkaRequest(apiKey = 0)
public class ProduceRequest implements RequestMessage {

    RequestHeader header;

    @Primitive(PrimitiveType.INT16)
    Integer acknowledgments;

    @Primitive(PrimitiveType.INT32)
    Integer timeout;

    @Sequence(TopicData.class)
    List<TopicData> topicData;
}
