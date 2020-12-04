package dev.dmco.test.kafka.io.codec.structs;

import dev.dmco.test.kafka.io.buffer.ResponseBuffer;
import dev.dmco.test.kafka.io.codec.Codec;
import dev.dmco.test.kafka.io.codec.context.CodecContext;
import dev.dmco.test.kafka.io.codec.context.ContextProperty;
import dev.dmco.test.kafka.io.codec.generic.ObjectCodec;
import dev.dmco.test.kafka.messages.common.Subscription;

import java.nio.ByteBuffer;
import java.util.List;

public class SubscriptionCodec implements Codec {

    private final Codec delegate = new ObjectCodec(Subscription.class);

    @Override
    public Object decode(ByteBuffer buffer, CodecContext context) {
        buffer.getInt();
        buffer.mark();
        int version = buffer.getShort();
        buffer.reset();
        CodecContext subscriptionContext = context.set(ContextProperty.VERSION, version);
        return delegate.decode(buffer, subscriptionContext);
    }

    @Override
    public void encode(Object value, ResponseBuffer buffer, CodecContext context) {
        Subscription subscription = (Subscription) value;
        CodecContext subscriptionContext = context.set(ContextProperty.VERSION, (int) subscription.version());
        ResponseBuffer subscriptionBuffer = new ResponseBuffer();
        delegate.encode(subscription, subscriptionBuffer, subscriptionContext);
        buffer.putInt(subscriptionBuffer.size());
        List<ByteBuffer> subscriptionBuffers = subscriptionBuffer.collect();
        buffer.putBuffers(subscriptionBuffers);
    }
}
