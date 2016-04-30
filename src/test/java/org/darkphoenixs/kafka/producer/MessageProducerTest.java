package org.darkphoenixs.kafka.producer;

import org.darkphoenixs.kafka.core.KafkaDestination;
import org.darkphoenixs.kafka.core.KafkaMessageTemplate;
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
		KafkaMessageTemplate<String> messageTemplate = new KafkaMessageTemplate<String>();
		producer.setMessageTemplate(messageTemplate);
		
		Assert.assertEquals("TempQueue", producer.getProducerKey());
		
		System.out.println(producer.doSend("test"));
	}
}
