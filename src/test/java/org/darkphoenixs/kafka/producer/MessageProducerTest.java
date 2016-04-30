package org.darkphoenixs.kafka.producer;

import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.core.KafkaMessageTemplate;
import org.darkphoenixs.mq.exception.MQException;
import org.junit.Assert;
import org.junit.Test;

public class MessageProducerTest {

	@Test
	public void test() throws Exception {
		
		MessageProducer<String> producer = new MessageProducer<String>();

		Assert.assertNull(producer.getProtocolId());
		producer.setProtocolId("ProtocolId");
		
		Assert.assertNull(producer.getDestination());
		KafkaDestination destination = new KafkaDestination("TempQueue");
		producer.setDestination(destination);
		
		Assert.assertNull(producer.getMessageTemplate());
		KafkaMessageTemplateImpl messageTemplate = new KafkaMessageTemplateImpl();
		producer.setMessageTemplate(messageTemplate);
		
		Assert.assertEquals("TempQueue", producer.getProducerKey());
		
		producer.send("test");
	}
	
	private class KafkaMessageTemplateImpl extends KafkaMessageTemplate<String> {
		
		@Override
		public void convertAndSend(KafkaDestination destination, String message)
				throws MQException {
			System.out.println(destination + ":" + message);
		}
	}
}
