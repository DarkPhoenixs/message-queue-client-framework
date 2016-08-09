package org.darkphoenixs.activemq.producer;

import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.darkphoenixs.mq.exception.MQException;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jms.JmsException;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;

public class MessageProducerTest {

    @Test
    public void test() throws Exception {

        MessageProducer<String> producer = new MessageProducer<String>();

        Assert.assertNull(producer.getDestination());
        Destination destination = new ActiveMQTempQueue("TempQueue");
        producer.setDestination(destination);

        Assert.assertNull(producer.getJmsTemplate());
        JmsTemplate jmsTemplate = new JmsTemplateImpl();
        producer.setJmsTemplate(jmsTemplate);

        Assert.assertEquals("TempQueue", producer.getProducerKey());

        producer.send("test");

        Destination destination2 = new ActiveMQTempTopic("TempTopic");
        producer.setDestination(destination2);

        JmsTemplate jmsTemplate2 = new JmsTemplateImpl2();
        producer.setJmsTemplate(jmsTemplate2);

        Assert.assertEquals("TempTopic", producer.getProducerKey());

        try {
            producer.send("test");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }

        producer.setDestination(new Destination() {
            @Override
            public String toString() {
                return "TempDestination";
            }
        });

        Assert.assertEquals("TempDestination", producer.getProducerKey());

        producer.setDestination(new Queue() {

            @Override
            public String getQueueName() throws JMSException {
                throw new JMSException("test");
            }
        });

        try {
            producer.getProducerKey();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }

        producer.setDestination(new Topic() {

            @Override
            public String getTopicName() throws JMSException {
                throw new JMSException("test");
            }
        });

        try {
            producer.getProducerKey();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }

        producer.setProducerKey("QUEUE.TEST");

        Assert.assertEquals("QUEUE.TEST", producer.getProducerKey());
    }

    private class JmsTemplateImpl extends JmsTemplate {

        @Override
        public void convertAndSend(Destination destination, Object message)
                throws JmsException {
            System.out.println(destination + ":" + message);
        }
    }

    private class JmsTemplateImpl2 extends JmsTemplate {

        @Override
        public void convertAndSend(Destination destination, Object message)
                throws JmsException {

            throw new JmsException("Test") {

                private static final long serialVersionUID = 1L;
            };
        }
    }
}
