package org.darkphoenixs.kafka.consumer;

import org.junit.Assert;
import org.junit.Test;

public class MessageConsumerTest {

	@Test
	public void test() throws Exception {

		MessageConsumer<String> consumer = new MessageConsumer<String>();

		consumer.setConsumerKey("ProtocolId");

		consumer.receive(consumer.getConsumerKey());

		Assert.assertEquals("ProtocolId", consumer.getConsumerKey());
	}
	
}
