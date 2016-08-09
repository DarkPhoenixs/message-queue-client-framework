package org.darkphoenixs.kafka.codec;

import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MessageEncoderTest {

    @Test
    public void test() throws Exception {

        MessageEncoderImpl encoder = new MessageEncoderImpl();

        MessageDecoderImpl decoder = new MessageDecoderImpl();

        MessageBeanImpl messageBean = new MessageBeanImpl();

        long date = System.currentTimeMillis();
        messageBean.setMessageNo("MessageNo");
        messageBean.setMessageType("MessageType");
        messageBean.setMessageAckNo("MessageAckNo");
        messageBean.setMessageDate(date);
        messageBean.setMessageContent("MessageContent".getBytes("UTF-8"));

        byte[] bytes = encoder.encode(messageBean);

        MessageBeanImpl messageBean2 = decoder.decode(bytes);

        Assert.assertEquals(messageBean.getMessageNo(),
                messageBean2.getMessageNo());
        Assert.assertEquals(messageBean.getMessageType(),
                messageBean2.getMessageType());
        Assert.assertEquals(messageBean.getMessageDate(),
                messageBean2.getMessageDate());
        Assert.assertEquals(messageBean.getMessageAckNo(),
                messageBean2.getMessageAckNo());
        Assert.assertArrayEquals(messageBean.getMessageContent(),
                messageBean.getMessageContent());

        List<MessageBeanImpl> list = new ArrayList<MessageBeanImpl>();
        list.add(messageBean);
        list.add(messageBean);

        List<byte[]> bytesList = encoder.batchEncode(list);

        List<MessageBeanImpl> list2 = decoder.batchDecode(bytesList);

        Assert.assertEquals(list.size(), list2.size());

        try {
            encoder.encode(null);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MQException);
        }
    }
}
