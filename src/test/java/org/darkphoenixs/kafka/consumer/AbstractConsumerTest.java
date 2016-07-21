package org.darkphoenixs.kafka.consumer;

import org.darkphoenixs.mq.exception.MQException;
import org.junit.Test;

public class AbstractConsumerTest {

	@Test
	public void test() throws Exception {

		AbstractConsumer<Integer, String> consumer = new MessageConsumer<Integer, String>();

		consumer.receive(2, "test");

		consumer.receive("");

		AbstractConsumer<Integer, String> consumer2 = new MessageConsumerDemo();

		try {
			consumer2.receive(2, "test");
		} catch (Exception e) {
		}

		try {
			consumer2.receive("");
		} catch (Exception e) {
		}

	}

	class MessageConsumerDemo extends AbstractConsumer<Integer, String> {

		@Override
		protected void doReceive(Integer key, String val) throws MQException {
			throw new RuntimeException("Test");
		}

		@Override
		protected void doReceive(String message) throws MQException {

			throw new RuntimeException("Test");

		}

	}
}
