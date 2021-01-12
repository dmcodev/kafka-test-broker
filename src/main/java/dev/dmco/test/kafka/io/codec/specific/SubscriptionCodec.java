package dev.dmco.test.kafka.io.codec.specific;

import dev.dmco.test.kafka.messages.consumer.Subscription;

public class SubscriptionCodec extends ConsumerMessageCodec {

    public SubscriptionCodec() {
        super(Subscription.class);
    }
}
