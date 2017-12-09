package org.darkphoenixs.mq.common;

import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.darkphoenixs.mq.producer.MQProducer;
import org.junit.Assert;
import org.junit.Test;

public class MessageProducerFactoryTest {

    @Test
    public void test_0() throws Exception {

        MQMessageProducerFactory factory = (MQMessageProducerFactory) MQMessageProducerFactory
                .getInstance();

        factory = (MQMessageProducerFactory) MQMessageProducerFactory
                .getInstance();

        factory.init();

        factory.destroy();

        ProducerTest producer1 = new ProducerTest();
        producer1.setProducerKey("ProducerKey1");

        ProducerTest producer2 = new ProducerTest();
        producer2.setProducerKey("ProducerKey2");

        factory.setProducers(new MQProducer[]{producer1});
        factory.addProducer(producer2);

        factory.init();

        Assert.assertEquals(producer1, factory.getProducer("ProducerKey1"));
        Assert.assertEquals(producer2, factory.getProducer("ProducerKey2"));
        Assert.assertNull(factory.getProducer("ProducerKey3"));

        factory.destroy();
    }

    private class ProducerTest implements MQProducer<MessageBeanImpl> {

        private String producerKey;

        @Override
        public String getProducerKey() throws MQException {

            return this.producerKey;
        }

        public void setProducerKey(String producerKey) {
            this.producerKey = producerKey;
        }

        @Override
        public void send(MessageBeanImpl message) throws MQException {
            System.out.println(message);
        }
    }
}
