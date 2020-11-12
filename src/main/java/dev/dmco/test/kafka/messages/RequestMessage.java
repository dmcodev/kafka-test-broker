package dev.dmco.test.kafka.messages;

import dev.dmco.test.kafka.messages.request.RequestHeader;

public interface RequestMessage {
    RequestHeader header();
}
