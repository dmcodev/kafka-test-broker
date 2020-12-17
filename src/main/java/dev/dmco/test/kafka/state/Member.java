package dev.dmco.test.kafka.state;

import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class Member {

    static final String NAME_PREFIX = "member";

    private final String id;
    private final Set<String> protocols;

    @Setter private Map<String, List<Integer>> assignments;

    public Set<String> getProtocols() {
        return new HashSet<>(protocols);
    }

    public MemberSnapshot getSnapshot() {
        return MemberSnapshot.builder()
            .id(id)
            .topics(
                Optional.ofNullable(assignments)
                    .map(Map::keySet)
                    .map(ArrayList::new)
                    .orElseGet(ArrayList::new)
            )
            .partitions(
                Optional.ofNullable(assignments)
                    .map(map -> map.entrySet().stream()
                        .filter(it -> !it.getValue().isEmpty())
                        .collect(Collectors.toMap(Map.Entry::getKey, it -> it.getValue().stream().collect(Collectors.toList())))
                    )
                    .orElseGet(HashMap::new)
            )
            .build();
    }
}
