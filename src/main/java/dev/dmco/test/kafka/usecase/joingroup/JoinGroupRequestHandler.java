package dev.dmco.test.kafka.usecase.joingroup;

import dev.dmco.test.kafka.messages.Subscription;
import dev.dmco.test.kafka.messages.Subscription.PartitionAssignments;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class JoinGroupRequestHandler implements RequestHandler<JoinGroupRequest, JoinGroupResponse> {

    @Override
    public List<Class<? extends JoinGroupRequest>> handledRequestTypes() {
        return Collections.singletonList(JoinGroupRequest.class);
    }

    @Override
    public JoinGroupResponse handle(JoinGroupRequest request, BrokerState state) {
        Subscription subscription = request.protocols().get(0).subscription();
        return JoinGroupResponse.builder()
            .errorCode((short) 0)
            .generationId(0)
            .protocolName(request.protocols().get(0).name())
            .leader("leader-id")
            .memberId("leader-id")
            .member(
                JoinGroupResponse.Member.builder()
                    .memberId("leader-id")
                    .subscription(
                        Subscription.builder()
                            .version(subscription.version())
                            .topics(subscription.topics())
                            .partitionAssignment(
                                PartitionAssignments.builder()
                                    .topicName(subscription.topics().get(0))
                                    .partitions(Arrays.asList(0))
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();
    }
}
