package dev.dmco.test.kafka.io.codec.structs;

import dev.dmco.test.kafka.io.codec.generic.VersionedTypeBytesCodec;
import dev.dmco.test.kafka.messages.Subscription;

public class SubscriptionCodec extends VersionedTypeBytesCodec {

    public SubscriptionCodec() {
        super(Subscription.class);
    }
}
