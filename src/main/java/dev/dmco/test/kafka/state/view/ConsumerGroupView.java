package dev.dmco.test.kafka.state.view;

import dev.dmco.test.kafka.state.ConsumerGroup;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
@Builder
public class ConsumerGroupView {

    String name;
    int membersCount;

    static ConsumerGroupView from(ConsumerGroup state) {
        return ConsumerGroupView.builder()
            .name(state.name())
            .membersCount(state.members().size())
            .build();
    }
}
