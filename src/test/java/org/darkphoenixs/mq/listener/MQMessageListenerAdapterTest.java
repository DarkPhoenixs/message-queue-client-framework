/*
 * Copyright (c) 2018. Dark Phoenixs (Open-Source Organization).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.darkphoenixs.mq.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.message.MessageExt;
import org.darkphoenixs.kafka.core.KafkaMessageAdapter;
import org.darkphoenixs.mq.codec.MQMessageDecoder;
import org.darkphoenixs.mq.codec.MQMessageDecoderAdapter;
import org.darkphoenixs.mq.consumer.MQConsumerAdapter;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.util.MQ_BATCH;
import org.darkphoenixs.mq.util.MQ_MODEL;
import org.darkphoenixs.mq.util.MQ_TYPE;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class MQMessageListenerAdapterTest {

    MQConsumerAdapter<String> mqConsumerAdapter = new MQConsumerAdapter<String>() {
        @Override
        protected void doReceive(String message) throws MQException {
            System.out.println(message);
        }
    };

    MQMessageDecoder<String> mqMessageDecoder = new MQMessageDecoderAdapter<String>() {
        @Override
        public String decode(byte[] bytes) throws MQException {
            return new String();
        }
    };

    @Test
    public void test_0() {
        MQMessageListenerAdapter<String> mqMessageListenerAdapter = new MQMessageListenerAdapter<String>();

        try {
            mqMessageListenerAdapter.onMessage("test");
        } catch (MQException e) {
            e.printStackTrace();
        }

        try {
            mqMessageListenerAdapter.onMessageWithKey("key", "test");
        } catch (MQException e) {
            e.printStackTrace();
        }

        try {
            mqMessageListenerAdapter.onMessageWithBatch(Collections.singletonMap("key", "test"));
        } catch (MQException e) {
            e.printStackTrace();
        }

        Assert.assertNull(mqMessageListenerAdapter.getKafkaMessageAdapter());
        Assert.assertNull(mqMessageListenerAdapter.getRocketMessageListener());
        Assert.assertNull(mqMessageListenerAdapter.getMessageDecoder());
        mqMessageListenerAdapter.setMessageDecoder(mqMessageDecoder);
        Assert.assertNull(mqMessageListenerAdapter.getConsumerAdapter());
        mqMessageListenerAdapter.setConsumerAdapter(mqConsumerAdapter);
        Assert.assertNotNull(mqMessageListenerAdapter.getBatch());
        mqMessageListenerAdapter.setBatch(MQ_BATCH.NON_BATCH.name());
        Assert.assertNotNull(mqMessageListenerAdapter.getModel());
        mqMessageListenerAdapter.setModel(MQ_MODEL.MODEL_1.name());

        Assert.assertNull(mqMessageListenerAdapter.getType());

        try {
            mqMessageListenerAdapter.setType(MQ_TYPE.KAFKA.name());
        } catch (MQException e) {
            e.printStackTrace();
        }

        Assert.assertNotNull(mqMessageListenerAdapter.getType());

        try {
            mqMessageListenerAdapter.onMessage("test");
        } catch (MQException e) {
            e.printStackTrace();
        }

        try {
            mqMessageListenerAdapter.onMessageWithKey("key", "test");
        } catch (MQException e) {
            e.printStackTrace();
        }

        try {
            mqMessageListenerAdapter.onMessageWithBatch(Collections.singletonMap("key", "test"));
        } catch (MQException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void test_1() {

        MQMessageListenerAdapter<String> mqMessageListenerAdapter = new MQMessageListenerAdapter<String>();

        try {
            mqMessageListenerAdapter.setType(MQ_TYPE.KAFKA.name());
        } catch (MQException e) {
            e.printStackTrace();
        }

        mqMessageListenerAdapter.setMessageDecoder(mqMessageDecoder);

        mqMessageListenerAdapter.setConsumerAdapter(mqConsumerAdapter);

        try {
            mqMessageListenerAdapter.setType(MQ_TYPE.KAFKA.name());
        } catch (MQException e) {
            e.printStackTrace();
        }

        KafkaMessageAdapter<String, String> kafkaMessageAdapter = mqMessageListenerAdapter.getKafkaMessageAdapter();

        try {
            kafkaMessageAdapter.messageAdapter(new ConsumerRecord<byte[], byte[]>("TEST", 1, 1, "KEY".getBytes(), "VAL".getBytes()));
        } catch (MQException e) {
            e.printStackTrace();
        }

        try {
            kafkaMessageAdapter.messageAdapter(new ConsumerRecord<byte[], byte[]>("TEST", 1, 1, null, "VAL".getBytes()));
        } catch (MQException e) {
            e.printStackTrace();
        }

        try {
            kafkaMessageAdapter.messageAdapter(new ConsumerRecords<byte[], byte[]>(Collections.singletonMap(new TopicPartition("TEST", 1), Collections.singletonList(new ConsumerRecord<byte[], byte[]>("TEST", 1, 1, "KEY".getBytes(), "VAL".getBytes())))));
        } catch (MQException e) {
            e.printStackTrace();
        }

        Map<byte[], byte[]> map = null;
        try {
            kafkaMessageAdapter.getDecoder().batchDecode(map);
        } catch (MQException e) {
            e.printStackTrace();
        }

        try {
            kafkaMessageAdapter.getDecoder().batchDecode(Collections.singletonList(new byte[0]));
        } catch (MQException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void test_2() {
        MQMessageListenerAdapter<String> mqMessageListenerAdapter = new MQMessageListenerAdapter<String>();

        try {
            mqMessageListenerAdapter.setType(MQ_TYPE.ROCKETMQ.name());
        } catch (MQException e) {
            e.printStackTrace();
        }

        mqMessageListenerAdapter.setMessageDecoder(mqMessageDecoder);

        mqMessageListenerAdapter.setConsumerAdapter(mqConsumerAdapter);

        try {
            mqMessageListenerAdapter.setType(MQ_TYPE.ROCKETMQ.name());
        } catch (MQException e) {
            e.printStackTrace();
        }

        MessageListenerOrderly messageListenerOrderly = (MessageListenerOrderly) mqMessageListenerAdapter.getRocketMessageListener();

        messageListenerOrderly.consumeMessage(Collections.singletonList(new MessageExt()), null);

        mqMessageListenerAdapter.setBatch(MQ_BATCH.BATCH.name());
        try {
            mqMessageListenerAdapter.setType(MQ_TYPE.ROCKETMQ.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        MessageListenerOrderly messageListenerOrderly2 = (MessageListenerOrderly) mqMessageListenerAdapter.getRocketMessageListener();

        messageListenerOrderly2.consumeMessage(Collections.singletonList(new MessageExt()), null);
    }

    @Test
    public void test_3() {
        MQMessageListenerAdapter<String> mqMessageListenerAdapter = new MQMessageListenerAdapter<String>();

        try {
            mqMessageListenerAdapter.setType(MQ_TYPE.ACTIVEMQ.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
    }
}