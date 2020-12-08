package dev.dmco.test.kafka.io.codec.structs;

import dev.dmco.test.kafka.messages.Subscription;

public class SubscriptionCodec extends VersionedCodec {

    public SubscriptionCodec() {
        super(Subscription.class);
    }
}
