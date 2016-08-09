package org.darkphoenixs.activemq.convertor;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jms.support.converter.MessageConversionException;

import javax.jms.*;
import java.io.Serializable;

public class MessageConverterTest {

    @Test
    public void test() throws Exception {

        MessageConverterImpl converter = new MessageConverterImpl();

        ActiveMQBytesMessage message = new ActiveMQBytesMessage();

        long date = System.currentTimeMillis();

        message.setReadOnlyBody(false);

        message.setStringProperty("MessageNo", "123");
        message.setStringProperty("MessageAckNo", "12345");
        message.setStringProperty("MessageType", "type");
        message.setLongProperty("MessageDate", date);
        message.writeBytes("test".getBytes());
        message.storeContent();
        message.setReadOnlyBody(true);

        MessageBeanImpl bean = (MessageBeanImpl) converter.fromMessage(message);

        Assert.assertEquals("123", bean.getMessageNo());
        Assert.assertEquals("12345", bean.getMessageAckNo());
        Assert.assertEquals("type", bean.getMessageType());
        Assert.assertEquals(date, bean.getMessageDate());
        Assert.assertArrayEquals("test".getBytes(), bean.getMessageContent());

        ActiveMQBytesMessage message2 = (ActiveMQBytesMessage) converter
                .toMessage(bean, new SessionImpl());
        message2.storeContent();
        message2.setReadOnlyBody(true);

        Assert.assertEquals("123", message2.getStringProperty("MessageNo"));
        Assert.assertEquals("12345", message2.getStringProperty("MessageAckNo"));
        Assert.assertEquals("type", message2.getStringProperty("MessageType"));
        Assert.assertEquals(date, message2.getLongProperty("MessageDate"));
        Assert.assertEquals("test".length(), message2.readBytes(new byte[1024]));

        try {
            converter.fromMessage(null);
        } catch (Exception e) {

            Assert.assertTrue(e instanceof MessageConversionException);
        }

        try {
            converter.toMessage(null, null);
        } catch (Exception e) {

            Assert.assertTrue(e instanceof MessageConversionException);
        }
    }

    private class SessionImpl implements Session {

        @Override
        public BytesMessage createBytesMessage() throws JMSException {

            return new ActiveMQBytesMessage();
        }

        @Override
        public MapMessage createMapMessage() throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Message createMessage() throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ObjectMessage createObjectMessage() throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ObjectMessage createObjectMessage(Serializable object)
                throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public StreamMessage createStreamMessage() throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TextMessage createTextMessage() throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TextMessage createTextMessage(String text) throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean getTransacted() throws JMSException {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public int getAcknowledgeMode() throws JMSException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public void commit() throws JMSException {
            // TODO Auto-generated method stub

        }

        @Override
        public void rollback() throws JMSException {
            // TODO Auto-generated method stub

        }

        @Override
        public void close() throws JMSException {
            // TODO Auto-generated method stub

        }

        @Override
        public void recover() throws JMSException {
            // TODO Auto-generated method stub

        }

        @Override
        public MessageListener getMessageListener() throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void setMessageListener(MessageListener listener)
                throws JMSException {
            // TODO Auto-generated method stub

        }

        @Override
        public void run() {
            // TODO Auto-generated method stub

        }

        @Override
        public MessageProducer createProducer(Destination destination)
                throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public MessageConsumer createConsumer(Destination destination)
                throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public MessageConsumer createConsumer(Destination destination,
                                              String messageSelector) throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public MessageConsumer createConsumer(Destination destination,
                                              String messageSelector, boolean NoLocal) throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Queue createQueue(String queueName) throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Topic createTopic(String topicName) throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TopicSubscriber createDurableSubscriber(Topic topic, String name)
                throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TopicSubscriber createDurableSubscriber(Topic topic,
                                                       String name, String messageSelector, boolean noLocal)
                throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public QueueBrowser createBrowser(Queue queue) throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public QueueBrowser createBrowser(Queue queue, String messageSelector)
                throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TemporaryQueue createTemporaryQueue() throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public TemporaryTopic createTemporaryTopic() throws JMSException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void unsubscribe(String name) throws JMSException {
            // TODO Auto-generated method stub

        }

    }
}
