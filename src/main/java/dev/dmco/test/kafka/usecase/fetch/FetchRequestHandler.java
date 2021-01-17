package dev.dmco.test.kafka.usecase.fetch;

import dev.dmco.test.kafka.messages.Record;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.Partition;
import dev.dmco.test.kafka.usecase.RequestHandler;
import dev.dmco.test.kafka.usecase.ResponseScheduler;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class FetchRequestHandler implements RequestHandler<FetchRequest, FetchResponse> {

    private static final long MIN_FETCH_LOOP_INTERVAL_MS = 10;

    @Override
    public void handle(FetchRequest request, BrokerState state, ResponseScheduler<FetchResponse> scheduler) {
        int maxWaitTime = request.maxWaitTime();
        long timeoutTimestamp = System.currentTimeMillis() + maxWaitTime;
        long fetchInterval = Math.max(MIN_FETCH_LOOP_INTERVAL_MS, maxWaitTime / 10);
        fetchLoop(request, state, scheduler, timeoutTimestamp, fetchInterval);
    }

    private void fetchLoop(
        FetchRequest request,
        BrokerState state,
        ResponseScheduler<FetchResponse> scheduler,
        long timeoutTimestamp,
        long fetchInterval
    ) {
        FetchResponse response = fetch(request, state);
        if (hasNoRecords(response) && fetchInterval > 0 && System.currentTimeMillis() < timeoutTimestamp) {
            scheduler.schedule(fetchInterval, () -> fetchLoop(request, state, scheduler, timeoutTimestamp, fetchInterval));
        } else {
            scheduler.scheduleResponse(response);
        }
    }

    private FetchResponse fetch(FetchRequest request, BrokerState state) {
        return FetchResponse.builder()
            .topics(
                request.topics().stream()
                    .map(requestTopic -> fetchTopic(requestTopic, state))
                    .collect(Collectors.toList())
            )
            .build();
    }

    private FetchResponse.Topic fetchTopic(FetchRequest.Topic requestTopic, BrokerState state) {
        return FetchResponse.Topic.builder()
            .name(requestTopic.name())
            .partitions(
                requestTopic.partitions().stream()
                    .map(requestPartition -> fetchPartition(requestTopic.name(), requestPartition, state))
                    .collect(Collectors.toList())
            )
            .build();
    }

    private FetchResponse.Partition fetchPartition(String topicName, FetchRequest.Partition requestPartition, BrokerState state) {
        Partition partition = state.getTopic(topicName).getPartition(requestPartition.partitionId());
        List<Record> records = partition.fetch(requestPartition.fetchOffset(), requestPartition.maxBytes());
        return FetchResponse.Partition.builder()
            .partitionId(partition.id())
            .headOffset(partition.head())
            .records(records)
            .build();
    }

    private boolean hasNoRecords(FetchResponse response) {
        return !response.topics().stream()
            .map(FetchResponse.Topic::partitions)
            .flatMap(Collection::stream)
            .map(FetchResponse.Partition::records)
            .flatMap(Collection::stream)
            .findAny()
            .isPresent();
    }
}
