package org.darkphoenixs.kafka.listener;


import org.darkphoenixs.kafka.consumer.MessageConsumer;
import org.junit.Assert;
import org.junit.Test;

public class MessageConsumerListenerTest {

	@Test
	public void test() throws Exception {
		
		MessageConsumerListener<String> listener = new MessageConsumerListener<String>();
		
		MessageConsumer<String> consumer = new MessageConsumer<String>();

		consumer.setTabName("TabName");
		consumer.setFuncName("FuncName");
		consumer.setIframeName("IframeName");
		consumer.setProtocolId("ProtocolId");
		
		Assert.assertNull(listener.getConsumer());
		listener.setConsumer(consumer);
		
		listener.onMessage("test");
	}
	
}
