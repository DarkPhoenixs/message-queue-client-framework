package org.darkphoenixs.mq.meaage;

import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.Assert;
import org.junit.Test;

public class MessageBeanTest {

	@Test
	public void test() throws Exception {
		
		MessageBeanImpl messageBean = new MessageBeanImpl();
		
		messageBean.setMessageNo("MessageNo");
		messageBean.setMessageType("MessageType");
		messageBean.setMessageAckNo("MessageAckNo");
		messageBean.setMessageDate(System.currentTimeMillis());
		messageBean.setMessageContent("MessageContent".getBytes());
		
		System.out.println(messageBean);
		
		Assert.assertEquals("MessageNo", messageBean.getMessageNo());
		Assert.assertEquals("MessageType", messageBean.getMessageType());
		Assert.assertEquals("MessageAckNo", messageBean.getMessageAckNo());
		Assert.assertNotEquals(System.currentTimeMillis(), messageBean.getMessageDate());
		Assert.assertArrayEquals("MessageContent".getBytes(), messageBean.getMessageContent());
	}
}
