package org.darkphoenixs.mq.producer;

import org.darkphoenixs.mq.exception.MQException;
import org.junit.Assert;
import org.junit.Test;

public class ProducerTest {

    @Test
    public void test() throws Exception {

        ProducerImpl producer = new ProducerImpl();

        producer.send(producer.getProducerKey());

        Assert.assertEquals("ProducerKey", producer.getProducerKey());

    }

    private class ProducerImpl implements MQProducer<String> {

        @Override
        public void send(String message) throws MQException {

            System.out.println(message);
        }

        @Override
        public String getProducerKey() throws MQException {

            return "ProducerKey";
        }
    }
}
