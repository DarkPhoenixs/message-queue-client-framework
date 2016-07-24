package org.darkphoenixs.kafka.listener;

import org.junit.Test;

public class KafkaMessageListenerTest {

	@Test
	public void test() throws Exception {
		
		KafkaMessageListenerDemo listener = new KafkaMessageListenerDemo();
		
		listener.onMessage("Test");
		
		listener.onMessage(1, "Test");
	}
	
	class KafkaMessageListenerDemo extends KafkaMessageListener<Integer, String> {
		
	}
}
