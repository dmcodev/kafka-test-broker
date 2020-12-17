package dev.dmco.test.kafka.state;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Map;

@Value
@Builder
@Accessors(fluent = true)
public class MemberSnapshot {
    String id;
    List<String> topics;
    Map<String, List<Integer>> partitions;
}
