package org.darkphoenixs.kafka.producer;

import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.core.KafkaMessageTemplate;
import org.darkphoenixs.mq.exception.MQException;
import org.junit.Assert;
import org.junit.Test;

public class MessageProducerTest {

    @Test
    public void test() throws Exception {

        MessageProducer<Integer, String> producer = new MessageProducer<Integer, String>();

        Assert.assertNull(producer.getDestination());
        KafkaDestination destination = new KafkaDestination("TempQueue");
        producer.setDestination(destination);

        Assert.assertNull(producer.getMessageTemplate());
        KafkaMessageTemplateImpl messageTemplate = new KafkaMessageTemplateImpl();
        producer.setMessageTemplate(messageTemplate);

        Assert.assertEquals("TempQueue", producer.getProducerKey());

        producer.send("test");

        producer.sendWithKey(1, "test");

        KafkaMessageTemplateImpl2 messageTemplate2 = new KafkaMessageTemplateImpl2();
        producer.setMessageTemplate(messageTemplate2);

        try {
            producer.send("err");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }

        try {
            producer.sendWithKey(2, "err");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }

        producer.setProducerKey("QUEUE.TEST");

        Assert.assertEquals("QUEUE.TEST", producer.getProducerKey());
    }

    private class KafkaMessageTemplateImpl extends
            KafkaMessageTemplate<Integer, String> {

        @Override
        public void convertAndSend(KafkaDestination destination, String message)
                throws MQException {
            System.out.println(destination + ":" + message);
        }

        @Override
        public void convertAndSendWithKey(KafkaDestination destination,
                                          Integer key, String message) throws MQException {

            System.out.println(destination + ":" + key + ":" + message);
        }
    }

    private class KafkaMessageTemplateImpl2 extends
            KafkaMessageTemplate<Integer, String> {

        @Override
        public void convertAndSend(KafkaDestination destination, String message)
                throws MQException {

            throw new MQException("test");
        }

        @Override
        public void convertAndSendWithKey(KafkaDestination destination,
                                          Integer key, String message) throws MQException {

            throw new MQException("test");
        }
    }
}
