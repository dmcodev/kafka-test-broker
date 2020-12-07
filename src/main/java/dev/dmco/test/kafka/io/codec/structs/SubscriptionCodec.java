package dev.dmco.test.kafka.io.codec.structs;

import dev.dmco.test.kafka.io.codec.generic.VersionedBytesCodec;
import dev.dmco.test.kafka.messages.Subscription;

public class SubscriptionCodec extends VersionedBytesCodec {

    public SubscriptionCodec() {
        super(Subscription.class);
    }
}
