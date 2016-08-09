package org.darkphoenixs.kafka.codec;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class KafkaMessageEncoderImplTest {

    @Test
    public void test() throws Exception {

        KafkaMessageEncoderImpl encoder = new KafkaMessageEncoderImpl();

        MessageBeanImpl messageBean = new MessageBeanImpl();

        long date = System.currentTimeMillis();
        messageBean.setMessageNo("MessageNo");
        messageBean.setMessageType("MessageType");
        messageBean.setMessageAckNo("MessageAckNo");
        messageBean.setMessageDate(date);
        messageBean.setMessageContent("MessageContent".getBytes("UTF-8"));

        encoder.encode(messageBean);
        encoder.encodeKey(1);
        encoder.encodeVal(messageBean);

        List<MessageBeanImpl> list = Lists.newArrayList(messageBean);

        encoder.batchEncode(list);

        Map<Integer, MessageBeanImpl> map = Maps.newHashMap();

        map.put(1, messageBean);

        encoder.batchEncode(map);
    }
}
