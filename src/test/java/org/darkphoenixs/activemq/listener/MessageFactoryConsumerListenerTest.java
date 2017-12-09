package org.darkphoenixs.activemq.listener;

import org.darkphoenixs.activemq.consumer.AbstractConsumer;
import org.darkphoenixs.mq.common.MQMessageConsumerFactory;
import org.darkphoenixs.mq.consumer.MQConsumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Executors;

public class MessageFactoryConsumerListenerTest {

    @Test
    public void test() throws Exception {

        MessageFactoryConsumerListener<MessageBeanImpl> factoryListener = new MessageFactoryConsumerListener<MessageBeanImpl>();

        try {
            factoryListener.onMessage(null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }

        MQMessageConsumerFactory consumerFactory = (MQMessageConsumerFactory) MQMessageConsumerFactory
                .getInstance();

        ConsumerTest consumer1 = new ConsumerTest();
        consumer1.setConsumerKey("ProtocolId1");

        ConsumerTest consumer2 = new ConsumerTest();
        consumer2.setConsumerKey("ProtocolId2");

        ConsumerTestErr consumerErr = new ConsumerTestErr();
        consumerErr.setConsumerKey("ProtocolIdErr");

        consumerFactory.setConsumers(new MQConsumer[]{consumer1, consumer2});
        consumerFactory.addConsumer(consumerErr);
        consumerFactory.init();

        Assert.assertNull(factoryListener.getConsumerFactory());
        factoryListener.setConsumerFactory(consumerFactory);

        try {
            factoryListener.onMessage(null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }

        Assert.assertNull(factoryListener.getConsumerKeyField());
        factoryListener.setConsumerKeyField("messageType");

        try {
            factoryListener.onMessage(null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }

        MessageBeanImpl messageBean1 = new MessageBeanImpl();

        messageBean1.setMessageNo("MessageNo1");
        messageBean1.setMessageType(null);
        messageBean1.setMessageAckNo("MessageAckNo1");
        messageBean1.setMessageDate(System.currentTimeMillis());
        messageBean1.setMessageContent("MessageContent1".getBytes());

        try {
            factoryListener.onMessage(messageBean1);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }

        messageBean1.setMessageType("ProtocolId3");

        try {
            factoryListener.onMessage(messageBean1);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }

        messageBean1.setMessageType("ProtocolId1");
        factoryListener.onMessage(messageBean1);

        MessageBeanImpl messageBean2 = new MessageBeanImpl();

        messageBean2.setMessageNo("MessageNo2");
        messageBean2.setMessageType("ProtocolId2");
        messageBean2.setMessageAckNo("MessageAckNo2");
        messageBean2.setMessageDate(System.currentTimeMillis());
        messageBean2.setMessageContent("MessageContent2".getBytes());

        Assert.assertNull(factoryListener.getThreadPool());
        factoryListener.setThreadPool(Executors.newCachedThreadPool());
        factoryListener.onMessage(messageBean2);

        messageBean2.setMessageType("ProtocolIdErr");
        factoryListener.onMessage(messageBean2);
    }

    private class ConsumerTest extends AbstractConsumer<MessageBeanImpl> {

        @Override
        protected void doReceive(MessageBeanImpl message) throws MQException {

            System.out.println(message);
        }
    }

    private class ConsumerTestErr extends AbstractConsumer<MessageBeanImpl> {

        @Override
        protected void doReceive(MessageBeanImpl message) throws MQException {

            throw new MQException("Test");
        }
    }
}
