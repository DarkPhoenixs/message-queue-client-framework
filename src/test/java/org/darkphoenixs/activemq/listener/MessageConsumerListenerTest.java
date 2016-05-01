package org.darkphoenixs.activemq.listener;

import java.util.concurrent.Executors;

import org.darkphoenixs.activemq.consumer.MessageConsumer;
import org.darkphoenixs.mq.consumer.Consumer;
import org.darkphoenixs.mq.exception.MQException;
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

		try {
			listener.onMessage("test");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof MQException);
		}

		Assert.assertNull(listener.getConsumer());
		listener.setConsumer(consumer);

		listener.onMessage("test");
		
		Assert.assertNull(listener.getThreadPool());
		listener.setThreadPool(Executors.newCachedThreadPool());
		listener.onMessage("test with thread pool");
		
		Thread.sleep(5000);
		
		MessageConsumerImpl consumerImpl = new MessageConsumerImpl();
		listener.setConsumer(consumerImpl);
		listener.onMessage("test with exception");
	}

	private class MessageConsumerImpl implements Consumer<String> {

		@Override
		public String getConsumerKey() throws MQException {
			return null;
		}
		
		@Override
		public void receive(String message) throws MQException {
			throw new MQException("Test");
		}
	}
}
