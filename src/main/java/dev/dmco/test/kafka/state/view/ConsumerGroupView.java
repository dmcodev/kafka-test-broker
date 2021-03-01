package dev.dmco.test.kafka.state.view;

import dev.dmco.test.kafka.state.ConsumerGroup;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;

@Value
@Builder
@Getter(AccessLevel.NONE)
public class ConsumerGroupView {

    String name;

    public String name() {
        return name;
    }

    public static ConsumerGroupView from(ConsumerGroup state) {
        return null;
    }
}
