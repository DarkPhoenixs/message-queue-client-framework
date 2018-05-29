package org.darkphoenixs.kafka.core;

import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.darkphoenixs.kafka.codec.KafkaMessageDecoderImpl;
import org.darkphoenixs.kafka.codec.KafkaMessageEncoderImpl;
import org.darkphoenixs.kafka.listener.MessageConsumerListener;
import org.darkphoenixs.mq.consumer.MQConsumer;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.message.MessageBeanImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class KafkaMessageAdapterTest {

    @Test
    public void test() throws Exception {

        KafkaMessageAdapter<Integer, MessageBeanImpl> adapter = new KafkaMessageAdapter<Integer, MessageBeanImpl>();

        Assert.assertNotNull(adapter.getBatch());
        adapter.setBatch("NON_BATCH");

        Assert.assertNotNull(adapter.getModel());
        adapter.setModel("MODEL_1");

        Assert.assertNull(adapter.getDecoder());
        adapter.setDecoder(new KafkaMessageDecoderImpl());

        Assert.assertNull(adapter.getDestination());
        adapter.setDestination(new KafkaDestination("QUEUE.TEST"));

        Assert.assertNull(adapter.getMessageListener());

        MessageConsumerListener<Integer, MessageBeanImpl> listener = new MessageConsumerListener<Integer, MessageBeanImpl>();
        listener.setConsumer(new MQConsumer<MessageBeanImpl>() {

            @Override
            public void receive(MessageBeanImpl message) throws MQException {
                System.out.println(message);
            }

            @Override
            public String getConsumerKey() throws MQException {
                return "";
            }
        });

        adapter.setMessageListener(listener);

        MessageBeanImpl messageBean = new MessageBeanImpl();

        long date = System.currentTimeMillis();
        messageBean.setMessageNo("MessageNo");
        messageBean.setMessageType("MessageType");
        messageBean.setMessageAckNo("MessageAckNo");
        messageBean.setMessageDate(date);
        messageBean.setMessageContent("MessageContent".getBytes("UTF-8"));

        try {
            adapter.messageAdapter(new MessageAndMetadata<Object, Object>("QUEUE.TEST", 0, null, 0, null, null, -1, TimestampType.CREATE_TIME));
        } catch (Exception e) {

            Assert.assertTrue(e instanceof NullPointerException);
        }

        KafkaMessageEncoderImpl encoder = new KafkaMessageEncoderImpl();


        try {
            adapter.messageAdapter(new ConsumerRecord<Object, Object>("QUEUE.TEST", 0, 1, encoder.encodeKey(1), encoder.encodeVal(new MessageBeanImpl())));
        } catch (Exception e) {

        }

        try {
            adapter.messageAdapter(new ConsumerRecords<Object, Object>(Collections.singletonMap(new TopicPartition("QUEUE.TEST", 0), Collections.singletonList(new ConsumerRecord<Object, Object>("QUEUE.TEST", 0, 1, encoder.encodeKey(1), encoder.encodeVal(new MessageBeanImpl()))))));
        } catch (Exception e) {

        }
    }
}
