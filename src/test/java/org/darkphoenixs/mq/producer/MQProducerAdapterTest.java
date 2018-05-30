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

package org.darkphoenixs.mq.producer;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.darkphoenixs.kafka.pool.KafkaMessageNewSenderPool;
import org.darkphoenixs.mq.codec.MQMessageEncoder;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.util.MQ_TYPE;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.jms.core.JmsTemplate;

import java.util.*;

public class MQProducerAdapterTest {

    MQProducerAdapter<String> mqProducerAdapter = new MQProducerAdapter<String>() {

        @Override
        protected String doSend(String message) throws MQException {
            return message;
        }
    };

    MQMessageEncoder<String> mqMessageEncoder = new MQMessageEncoder<String>() {

        @Override
        public byte[] encode(String message) throws MQException {
            return message.getBytes();
        }

        @Override
        public List<byte[]> batchEncode(List<String> message) throws MQException {
            List<byte[]> list = new ArrayList<byte[]>();
            for (String str : message) {
                list.add(str.getBytes());
            }
            return list;
        }
    };

    @Test
    public void getActivemqTemplate() {
        Assert.assertNull(mqProducerAdapter.getActivemqTemplate());
    }

    @Test
    public void setActivemqTemplate() {
        mqProducerAdapter.setActivemqTemplate(new JmsTemplate());
    }

    @Test
    public void getActivemqDestination() {
        Assert.assertNull(mqProducerAdapter.getActivemqDestination());
    }

    @Test
    public void setActivemqDestination() {
        mqProducerAdapter.setActivemqDestination(new ActiveMQQueue("TEST"));
    }

    @Test
    public void getKafkaMessageSenderPool() {
        Assert.assertNull(mqProducerAdapter.getKafkaMessageSenderPool());
    }

    @Test
    public void setKafkaMessageSenderPool() {
        mqProducerAdapter.setKafkaMessageSenderPool(new KafkaMessageNewSenderPool<byte[], byte[]>());
    }

    @Test
    public void getRocketmqDefaultProducer() {
        Assert.assertNull(mqProducerAdapter.getRocketmqDefaultProducer());
    }

    @Test
    public void setRocketmqDefaultProducer() {
        mqProducerAdapter.setRocketmqDefaultProducer(new DefaultMQProducer());
    }

    @Test
    public void getRocketmqTransactionProducer() {
        Assert.assertNull(mqProducerAdapter.getRocketmqTransactionProducer());

    }

    @Test
    public void setRocketmqTransactionProducer() {
        mqProducerAdapter.setRocketmqTransactionProducer(new TransactionMQProducer());
    }

    @Test
    public void getMessageEncoder() {
        Assert.assertNull(mqProducerAdapter.getMessageEncoder());
    }

    @Test
    public void setMessageEncoder() {
        mqProducerAdapter.setMessageEncoder(mqMessageEncoder);
    }

    @Test
    public void getTopic() {
        Assert.assertNull(mqProducerAdapter.getTopic());
    }

    @Test
    public void setTopic() {
        mqProducerAdapter.setTopic("TEST");
    }

    @Test
    public void getType() {
        Assert.assertNull(mqProducerAdapter.getType());

        try {
            mqProducerAdapter.setType(MQ_TYPE.KAFKA.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        Assert.assertNotNull(mqProducerAdapter.getType());
    }

    @Test
    public void setType() {
        try {
            mqProducerAdapter.setType(MQ_TYPE.KAFKA.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void getProducerKey() {
        Assert.assertNull(mqProducerAdapter.getProducerKey());

    }

    @Test
    public void setProducerKey() {
        mqProducerAdapter.setProducerKey("ProducerKey");
    }

    @Test
    public void doSend() {
        try {
            mqProducerAdapter.doSend(Collections.singletonList("test"));
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void initProducerKafka() {

        try {
            mqProducerAdapter.setType(MQ_TYPE.KAFKA.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.send("test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.sendWithKey("key", "test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.batchSend(Collections.singletonList("test"));
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqProducerAdapter.setTopic("TEST");
        try {
            mqProducerAdapter.setType(MQ_TYPE.KAFKA.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqProducerAdapter.setMessageEncoder(mqMessageEncoder);
        try {
            mqProducerAdapter.setType(MQ_TYPE.KAFKA.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqProducerAdapter.setKafkaMessageSenderPool(new KafkaMessageNewSenderPool<byte[], byte[]>());

        try {
            mqProducerAdapter.setType(MQ_TYPE.KAFKA.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.send("test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.sendWithKey("key", "test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.sendWithKey(null, "test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.batchSend(Collections.singletonList("test"));
        } catch (MQException e) {
            e.printStackTrace();
        }

        org.darkphoenixs.kafka.producer.AbstractProducer<String, String> mqProducer = (org.darkphoenixs.kafka.producer.AbstractProducer<String, String>) mqProducerAdapter.getProducerInstance();
        org.darkphoenixs.kafka.codec.KafkaMessageEncoder<String, String> kafkaMessageEncoder = mqProducer.getMessageTemplate().getEncoder();

        try {
            Map<String, String> map = new HashMap<String, String>();
            map = null;
            kafkaMessageEncoder.batchEncode(map);
            kafkaMessageEncoder.batchEncode(Collections.singletonMap("key", "test"));
        } catch (MQException e) {
            e.printStackTrace();
        }

        try {
            kafkaMessageEncoder.batchEncode(Collections.singletonMap("key", "test"));
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void initProducerRocketMQ() {
        try {
            mqProducerAdapter.setType(MQ_TYPE.ROCKETMQ.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.send("test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.sendWithKey("key", "test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.batchSend(Collections.singletonList("test"));
        } catch (MQException e) {
            e.printStackTrace();
        }

        mqProducerAdapter.setTopic("TEST");
        try {
            mqProducerAdapter.setType(MQ_TYPE.ROCKETMQ.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqProducerAdapter.setMessageEncoder(mqMessageEncoder);
        try {
            mqProducerAdapter.setType(MQ_TYPE.ROCKETMQ.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqProducerAdapter.setRocketmqDefaultProducer(new DefaultMQProducer());
        try {
            mqProducerAdapter.setType(MQ_TYPE.ROCKETMQ.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqProducerAdapter.setRocketmqTransactionProducer(new TransactionMQProducer());
        try {
            mqProducerAdapter.setType(MQ_TYPE.ROCKETMQ.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.send("test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.sendWithKey("key", "test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.sendWithKey(null, "test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.batchSend(Collections.singletonList("test"));
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void initProducerActiveMQ() {

        try {
            mqProducerAdapter.setType(MQ_TYPE.ACTIVEMQ.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.send("test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.sendWithKey("key", "test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.batchSend(Collections.singletonList("test"));
        } catch (MQException e) {
            e.printStackTrace();
        }

        mqProducerAdapter.setTopic("TEST");
        mqProducerAdapter.setActivemqDestination(new ActiveMQQueue("TEST"));
        try {
            mqProducerAdapter.setType(MQ_TYPE.ACTIVEMQ.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqProducerAdapter.setActivemqTemplate(new JmsTemplate());

        try {
            mqProducerAdapter.setType(MQ_TYPE.ACTIVEMQ.name());
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.send("test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.sendWithKey("key", "test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.sendWithKey(null, "test");
        } catch (MQException e) {
            e.printStackTrace();
        }
        try {
            mqProducerAdapter.batchSend(Collections.singletonList("test"));
        } catch (MQException e) {
            e.printStackTrace();
        }
    }
}