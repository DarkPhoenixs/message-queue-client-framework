package org.darkphoenixs.kafka.listener;

import org.darkphoenixs.mq.exception.MQException;
import org.junit.Test;

public class KafkaMessageListenerTest {

	@Test
	public void test() throws Exception {
		
		KafkaMessageListenerDemo listener = new KafkaMessageListenerDemo();
		
		listener.onMessage("Test");
		
		listener.onMessage(1, "Test");
	}
	
	class KafkaMessageListenerDemo extends KafkaMessageListener<Integer, String> {
		
		@Override
		public void onMessage(Integer key, String val) throws MQException {
			
			System.out.println(key + ":" + val);
		}
		
		@Override
		public void onMessage(String message) throws MQException {

			System.out.println(message);
		}
	}
}
