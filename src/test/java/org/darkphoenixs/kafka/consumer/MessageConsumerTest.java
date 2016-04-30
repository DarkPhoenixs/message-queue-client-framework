package org.darkphoenixs.kafka.consumer;

import org.junit.Assert;
import org.junit.Test;

public class MessageConsumerTest {

	@Test
	public void test() throws Exception {

		MessageConsumer<String> consumer = new MessageConsumer<String>();

		consumer.setTabName("TabName");
		consumer.setFuncName("FuncName");
		consumer.setIframeName("IframeName");
		consumer.setProtocolId("ProtocolId");

		consumer.receive(consumer.getConsumerKey() + " "
				+ consumer.getFuncName() + " " + consumer.getIframeName() + " "
				+ consumer.getTabName());

		Assert.assertEquals(consumer.getConsumerKey(), consumer.getProtocolId());
	}
	
}
