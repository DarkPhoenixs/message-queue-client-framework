package org.darkphoenixs.kafka.listener;

import org.darkphoenixs.kafka.consumer.MessageConsumer;
import org.darkphoenixs.mq.exception.MQException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class KafkaMessageConsumerListenerTest {

    @Test
    public void test() throws Exception {

        KafkaMessageConsumerListener<Integer, String> listener = new KafkaMessageConsumerListener<Integer, String>();

        try {
            listener.onMessage(1, "test");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }
        try {
            listener.onMessage(new HashMap<Integer, String>());
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }

        MessageConsumer<Integer, String> consumer = new MessageConsumer<Integer, String>();

        consumer.setConsumerKey("ProtocolId");

        Assert.assertNull(listener.getConsumer());

        listener.setConsumer(consumer);

        listener.onMessage(1, "test");

        listener.onMessage(new HashMap<Integer, String>());
    }
}
