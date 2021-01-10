package dev.dmco.test.kafka.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class ProtocolSet {

    private final Set<String> protocolNames = new HashSet<>();

    public ProtocolSet() {}

    public ProtocolSet(Collection<String> protocolNames) {
        setProtocolNames(protocolNames);
    }

    public Collection<String> protocolNames() {
        return new ArrayList<>(protocolNames);
    }

    public boolean containsProtocolName(String protocolName) {
        return protocolNames.contains(protocolName);
    }

    public void setProtocolNames(Collection<String> protocolNames) {
        this.protocolNames.clear();
        this.protocolNames.addAll(protocolNames);
    }

    public ProtocolSet merge(ProtocolSet other) {
        Set<String> mergedProtocolNames = new HashSet<>(protocolNames);
        mergedProtocolNames.addAll(other.protocolNames);
        return new ProtocolSet(mergedProtocolNames);
    }

    public String findMatchingProtocolName(ProtocolSet candidateSet) {
        return candidateSet.protocolNames.stream()
            .filter(this::containsProtocolName)
            .findFirst()
            .orElse(null);
    }
}
