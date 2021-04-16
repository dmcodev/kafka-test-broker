package dev.dmcode.test.kafka.io.codec.specific;

import dev.dmcode.test.kafka.messages.consumer.Subscription;

public class SubscriptionCodec extends ConsumerMessageCodec {

    public SubscriptionCodec() {
        super(Subscription.class);
    }
}
