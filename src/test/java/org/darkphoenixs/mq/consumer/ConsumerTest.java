package org.darkphoenixs.mq.consumer;

import org.darkphoenixs.mq.exception.MQException;
import org.junit.Assert;
import org.junit.Test;

public class ConsumerTest {

	@Test
	public void test() throws Exception {

		ConsumerImpl consumer = new ConsumerImpl();

		consumer.setTabName("TabName");
		consumer.setFuncName("FuncName");
		consumer.setIframeName("IframeName");
		consumer.setProtocolId("ProtocolId");

		consumer.receive(consumer.getConsumerKey() + " "
				+ consumer.getFuncName() + " " + consumer.getIframeName() + " "
				+ consumer.getTabName());

		Assert.assertEquals(consumer.getConsumerKey(), consumer.getProtocolId());
	}

	private class ConsumerImpl extends AbstractConsumer<String> {

		@Override
		protected void doReceive(String message) throws MQException {
			System.out.println(message);
		}
	}
}
