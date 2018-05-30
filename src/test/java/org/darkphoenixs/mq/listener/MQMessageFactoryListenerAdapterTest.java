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
import org.darkphoenixs.kafka.core.KafkaMessageAdapter;
import org.darkphoenixs.mq.codec.MQMessageDecoder;
import org.darkphoenixs.mq.common.MQMessageConsumerFactory;
import org.darkphoenixs.mq.consumer.MQConsumerAdapter;
import org.darkphoenixs.mq.exception.MQException;
import org.darkphoenixs.mq.factory.MQConsumerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class MQMessageFactoryListenerAdapterTest {

    MQConsumerAdapter<Bean> mqConsumerAdapter = new MQConsumerAdapter<Bean>() {
        @Override
        protected void doReceive(Bean message) throws MQException {
            System.out.println(message.getMessage());
        }
    };

    MQMessageDecoder<Bean> mqMessageDecoder = new MQMessageDecoder<Bean>() {
        @Override
        public Bean decode(byte[] bytes) throws MQException {
            Bean bean = new Bean();
            bean.setField("test");
            bean.setMessage(new String(bytes));
            return bean;
        }

        @Override
        public List<Bean> batchDecode(List<byte[]> bytes) throws MQException {
            Bean bean = new Bean();
            bean.setField("test");
            bean.setMessage("message");
            return Collections.singletonList(bean);
        }
    };

    @Test
    public void test_0() {

        MQMessageFactoryListenerAdapter<Bean> mqMessageFactoryListenerAdapter = new MQMessageFactoryListenerAdapter<Bean>();

        Assert.assertNull(mqMessageFactoryListenerAdapter.getConsumerKeyField());
        Assert.assertNull(mqMessageFactoryListenerAdapter.getConsumerFactory());


        Bean bean = null;
        try {
            mqMessageFactoryListenerAdapter.onMessage(bean);
        } catch (MQException e) {
            e.printStackTrace();
        }
        bean = new Bean();
        try {
            mqMessageFactoryListenerAdapter.onMessage(bean);
        } catch (MQException e) {
            e.printStackTrace();
        }

        mqMessageFactoryListenerAdapter.setConsumerFactory(MQMessageConsumerFactory.getInstance());
        try {
            mqMessageFactoryListenerAdapter.onMessage(bean);
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqMessageFactoryListenerAdapter.setConsumerKeyField("field");
        try {
            mqMessageFactoryListenerAdapter.onMessage(bean);
        } catch (MQException e) {
            e.printStackTrace();
        }
        bean.setField("test");
        try {
            mqMessageFactoryListenerAdapter.onMessage(bean);
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqConsumerAdapter.setConsumerKey("test");
        MQConsumerFactory consumerFactory = MQMessageConsumerFactory.getInstance();
        try {
            consumerFactory.addConsumer(mqConsumerAdapter);
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqMessageFactoryListenerAdapter.setConsumerFactory(consumerFactory);
        try {
            mqMessageFactoryListenerAdapter.onMessage(bean);
        } catch (MQException e) {
            e.printStackTrace();
        }
        bean.setField("test1");
        try {
            mqMessageFactoryListenerAdapter.onMessage(bean);
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_1() {

        MQMessageFactoryListenerAdapter<Bean> mqMessageFactoryListenerAdapter = new MQMessageFactoryListenerAdapter<Bean>();

        Assert.assertNull(mqMessageFactoryListenerAdapter.getConsumerKeyField());
        Assert.assertNull(mqMessageFactoryListenerAdapter.getConsumerFactory());


        Bean bean = null;
        try {
            mqMessageFactoryListenerAdapter.onMessageWithKey("key", bean);
        } catch (MQException e) {
            e.printStackTrace();
        }
        bean = new Bean();
        try {
            mqMessageFactoryListenerAdapter.onMessageWithKey("key", bean);
        } catch (MQException e) {
            e.printStackTrace();
        }

        mqMessageFactoryListenerAdapter.setConsumerFactory(MQMessageConsumerFactory.getInstance());
        try {
            mqMessageFactoryListenerAdapter.onMessageWithKey("key", bean);
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqMessageFactoryListenerAdapter.setConsumerKeyField("field");
        try {
            mqMessageFactoryListenerAdapter.onMessageWithKey("key", bean);
        } catch (MQException e) {
            e.printStackTrace();
        }
        bean.setField("test");
        try {
            mqMessageFactoryListenerAdapter.onMessageWithKey("key", bean);
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqConsumerAdapter.setConsumerKey("test");
        MQConsumerFactory consumerFactory = MQMessageConsumerFactory.getInstance();
        try {
            consumerFactory.addConsumer(mqConsumerAdapter);
        } catch (MQException e) {
            e.printStackTrace();
        }
        mqMessageFactoryListenerAdapter.setConsumerFactory(consumerFactory);
        try {
            mqMessageFactoryListenerAdapter.onMessageWithKey("key", bean);
        } catch (MQException e) {
            e.printStackTrace();
        }
        bean.setField("test1");
        try {
            mqMessageFactoryListenerAdapter.onMessageWithKey("key", bean);
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_2() {
        MQMessageFactoryListenerAdapter<Bean> mqMessageFactoryListenerAdapter = new MQMessageFactoryListenerAdapter<Bean>();
        try {
            mqMessageFactoryListenerAdapter.onMessageWithBatch(Collections.singletonMap("key", new Bean()));
        } catch (MQException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_3() {

        MQMessageFactoryListenerAdapter<Bean> mqMessageFactoryListenerAdapter = new MQMessageFactoryListenerAdapter<Bean>();

        mqConsumerAdapter.setConsumerKey("test");

        MQConsumerFactory consumerFactory = MQMessageConsumerFactory.getInstance();
        try {
            consumerFactory.addConsumer(mqConsumerAdapter);
        } catch (MQException e) {
            e.printStackTrace();
        }

        mqMessageFactoryListenerAdapter.setConsumerFactory(consumerFactory);
        mqMessageFactoryListenerAdapter.setConsumerKeyField("field");
        mqMessageFactoryListenerAdapter.setMessageDecoder(mqMessageDecoder);
        try {
            mqMessageFactoryListenerAdapter.setType("KAFKA");
        } catch (MQException e) {
            e.printStackTrace();
        }

        KafkaMessageAdapter<String, Bean> kafkaMessageAdapter = mqMessageFactoryListenerAdapter.getKafkaMessageAdapter();

        try {
            kafkaMessageAdapter.messageAdapter(new ConsumerRecord("TEST", 1, 1, "KEY".getBytes(), "VAL".getBytes()));
        } catch (MQException e) {
            e.printStackTrace();
        }

    }

    class Bean {

        private String field;

        private String message;

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}