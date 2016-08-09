package org.darkphoenixs.kafka.codec;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class KafkaMessageDecoderImplTest {

    @Test
    public void test() throws Exception {

        KafkaMessageEncoderImpl encoder = new KafkaMessageEncoderImpl();

        KafkaMessageDecoderImpl decoder = new KafkaMessageDecoderImpl();

        MessageBeanImpl messageBean = new MessageBeanImpl();

        long date = System.currentTimeMillis();
        messageBean.setMessageNo("MessageNo");
        messageBean.setMessageType("MessageType");
        messageBean.setMessageAckNo("MessageAckNo");
        messageBean.setMessageDate(date);
        messageBean.setMessageContent("MessageContent".getBytes("UTF-8"));

        byte[] bytes = encoder.encode(messageBean);


        decoder.decode(bytes);
        decoder.decodeKey("1".getBytes());
        decoder.decodeVal(bytes);

        List<byte[]> list = Lists.newArrayList(bytes);

        decoder.batchDecode(list);

        Map<byte[], byte[]> map = Maps.newHashMap();

        map.put("1".getBytes(), bytes);

        decoder.batchDecode(map);
    }
}
