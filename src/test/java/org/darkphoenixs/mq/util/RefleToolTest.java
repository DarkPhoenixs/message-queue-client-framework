package org.darkphoenixs.mq.util;

import kafka.serializer.Decoder;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.Assert;
import org.junit.Test;

public class RefleToolTest {

    @Test
    public void test() throws Exception {

        Assert.assertNotNull(new RefleTool());

        Assert.assertTrue(RefleTool.newInstance(DefaultDecoder.class,
                new VerifiableProperties()) instanceof Decoder);

        try {
            RefleTool.newInstance(TestInter.class);

        } catch (Exception e) {
            Assert.assertTrue(e instanceof NoSuchMethodException);
        }


        MessageBeanImpl messageBean = new MessageBeanImpl();

        long date = System.currentTimeMillis();
        messageBean.setMessageNo("MessageNo");
        messageBean.setMessageType("MessageType");
        messageBean.setMessageAckNo("MessageAckNo");
        messageBean.setMessageDate(date);
        messageBean.setMessageContent("MessageContent".getBytes("UTF-8"));

        Assert.assertEquals("MessageType", RefleTool.getFieldValue(messageBean, "messageType"));

        Assert.assertEquals("MessageType", RefleTool.getMethodValue(messageBean, "getMessageType"));

        Assert.assertNull(RefleTool.getFieldValue(messageBean, "messageType1"));

        try {
            RefleTool.getMethodValue(messageBean, "messageType1");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NoSuchMethodException);
        }

    }

}

abstract class TestInter {

    public TestInter() {
    }
}