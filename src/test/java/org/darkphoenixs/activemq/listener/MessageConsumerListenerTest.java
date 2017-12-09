package org.darkphoenixs.activemq.listener;

import org.darkphoenixs.activemq.consumer.MessageConsumer;
import org.darkphoenixs.mq.consumer.MQConsumer;
import org.darkphoenixs.mq.exception.MQException;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Executors;

public class MessageConsumerListenerTest {

    @Test
    public void test() throws Exception {

        MessageConsumerListener<String> listener = new MessageConsumerListener<String>();

        MessageConsumer<String> consumer = new MessageConsumer<String>();

        consumer.setConsumerKey("consumerKey");

        try {
            listener.onMessage("test");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }

        Assert.assertNull(listener.getConsumer());
        listener.setConsumer(consumer);

        listener.onMessage("test");

        Assert.assertNull(listener.getThreadPool());
        listener.setThreadPool(Executors.newCachedThreadPool());
        listener.onMessage("test with thread pool");

        MessageConsumerListener<String> listener1 = new MessageConsumerListener<String>();
        MessageConsumerImpl consumerImpl = new MessageConsumerImpl();
        listener1.setConsumer(consumerImpl);
        listener1.setThreadPool(Executors.newCachedThreadPool());
        listener1.onMessage("test with exception");

        Thread.sleep(2000);

    }

    private class MessageConsumerImpl implements MQConsumer<String> {

        @Override
        public String getConsumerKey() throws MQException {
            return null;
        }

        @Override
        public void receive(String message) throws MQException {
            throw new MQException("Test");
        }
    }
}
