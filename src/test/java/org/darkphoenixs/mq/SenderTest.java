package org.darkphoenixs.mq;

import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.darkphoenixs.mq.producer.Producer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/kafka/applicationContext-producer.xml" })
public class SenderTest {

	@Autowired
	private Producer<MessageBeanImpl> producer;

	@Test
	public void test() throws Exception {

		for (int i = 0; i < 10; i++) {

			MessageBeanImpl message = new MessageBeanImpl();

			message.setMessageNo("MessageNo" + i);
			message.setMessageAckNo("MessageAckNo" + i);
			message.setMessageType("MessageType");
			message.setMessageContent("MessageTest".getBytes());
			message.setMessageDate(System.currentTimeMillis());

			producer.send(message);

		}
		
		Thread.sleep(Integer.MAX_VALUE);

	}
}