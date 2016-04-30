package org.darkphoenixs.activemq.producer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jms.core.JmsTemplate;

public class MessageProducerTest {

	@Test
	public void test() throws Exception {

		MessageProducer<String> producer = new MessageProducer<String>();

		Assert.assertNull(producer.getProtocolId());
		producer.setProtocolId("ProtocolId");

		Assert.assertNull(producer.getDestination());
		Destination destination = new ActiveMQTempQueue("TempQueue");
		producer.setDestination(destination);

		Assert.assertNull(producer.getJmsTemplate());
		JmsTemplate jmsTemplate = new JmsTemplate(new ConnectionFactory() {

			@Override
			public Connection createConnection(String userName, String password)
					throws JMSException {
				return new ActiveMQConnectionFactory().createConnection(
						userName, password);
			}

			@Override
			public Connection createConnection() throws JMSException {

				return new ActiveMQConnectionFactory().createConnection();
			}
		});
		producer.setJmsTemplate(jmsTemplate);

		Assert.assertEquals("TempQueue", producer.getProducerKey());

		System.out.println(producer.doSend("test"));
	}
}
