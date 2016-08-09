package org.darkphoenixs.mq.message;

import org.junit.Assert;
import org.junit.Test;

public class MessageBeanTest {

    @Test
    public void test() throws Exception {

        MessageBeanImpl messageBean = new MessageBeanImpl();

        long date = System.currentTimeMillis();
        messageBean.setMessageNo("MessageNo");
        messageBean.setMessageType("MessageType");
        messageBean.setMessageAckNo("MessageAckNo");
        messageBean.setMessageDate(date);
        messageBean.setMessageContent("MessageContent".getBytes("UTF-8"));

        Assert.assertEquals("MessageNo", messageBean.getMessageNo());
        Assert.assertEquals("MessageType", messageBean.getMessageType());
        Assert.assertEquals("MessageAckNo", messageBean.getMessageAckNo());
        Assert.assertEquals(date, messageBean.getMessageDate());
        Assert.assertArrayEquals("MessageContent".getBytes("UTF-8"), messageBean.getMessageContent());

        System.out.println(messageBean);
    }
}
