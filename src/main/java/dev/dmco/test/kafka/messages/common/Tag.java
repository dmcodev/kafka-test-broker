package dev.dmco.test.kafka.messages.common;

import lombok.AllArgsConstructor;

@lombok.Value
@AllArgsConstructor
public class Tag {
    int key;
    byte[] value;
}
