package dev.dmco.test.kafka.messages.data;

import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Accessors(fluent = true)
public class TopicData {
    String topic;
    List<PartitionRecordSet> data;
}
