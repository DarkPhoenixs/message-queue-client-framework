package org.darkphoenixs.mq.common;

import org.darkphoenixs.mq.consumer.AbstractConsumer;
import org.darkphoenixs.mq.consumer.Consumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.Assert;
import org.junit.Test;

public class MessageConsumerFactoryTest {

	@Test
	public void test_0() throws Exception {

		MessageConsumerFactory factory = (MessageConsumerFactory) MessageConsumerFactory
				.getInstance();

		ConsumerTest consumer1 = new ConsumerTest();
		consumer1.setTabName("TabName1");
		consumer1.setFuncName("FuncName1");
		consumer1.setIframeName("IframeName1");
		consumer1.setProtocolId("ProtocolId1");

		ConsumerTest consumer2 = new ConsumerTest();
		consumer2.setTabName("TabName2");
		consumer2.setFuncName("FuncName2");
		consumer2.setIframeName("IframeName2");
		consumer2.setProtocolId("ProtocolId2");
		
		factory.setConsumers(new Consumer[] { consumer1 });
		factory.addConsumer(consumer2);
		factory.init();
		
		Assert.assertEquals(consumer1, factory.getConsumer("ProtocolId1"));
		Assert.assertEquals(consumer2, factory.getConsumer("ProtocolId2"));
		Assert.assertNull(factory.getConsumer("ProtocolId3"));

		
		factory.destroy();

	}

	private class ConsumerTest extends AbstractConsumer<MessageBeanImpl> {

		@Override
		protected void doReceive(MessageBeanImpl message) throws MQException {
			
			System.out.println(message);
		}
	}
}
